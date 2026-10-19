/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.shuffle;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * The hash the row path gives a row of a ReduceSink, Murmur3.hash32(seed 0) of its BinarySortable key bytes, computed
 * from the key columns without writing the bytes, for keys of fixed width: BOOLEAN, TINYINT, SMALLINT, INT, DATE,
 * BIGINT. A key column is its marker byte (null or not null), then for a value its bytes: boolean 2 / 1, the others
 * big endian with the sign bit flipped; markers and values are inverted for a descending column. A row whose keys
 * are all null therefore hashes as the row path's null key does.
 */
public final class BinarySortableKeyHash {

  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  /** The BinarySortable width of a key column: 0 for BOOLEAN, else the bytes of its value. */
  private final int[] widths;
  private final int[] columns;
  private final byte[] nullMarkers;
  private final byte[] notNullMarkers;
  private final boolean[] descending;

  // the Murmur3 state of the row being hashed
  private int hash;
  private int word;
  private int wordBytes;
  private int length;
  // the bytes of the incomplete word, signed: Murmur3's tail does not mask them
  private final byte[] tail = new byte[3];

  /**
   * @param columns the batch columns of the keys
   * @param widths per key 0 (BOOLEAN), 1 (TINYINT), 2 (SMALLINT), 4 (INT, DATE) or 8 (BIGINT)
   */
  public BinarySortableKeyHash(int[] columns, int[] widths, byte[] nullMarkers, byte[] notNullMarkers,
      boolean[] descending) {
    this.columns = columns;
    this.widths = widths;
    this.nullMarkers = nullMarkers;
    this.notNullMarkers = notNullMarkers;
    this.descending = descending;
  }

  /** The hash of every row of the batch (by batch index, honoring the selection). */
  public void hash(VectorizedRowBatch batch, int[] hashes) {
    int[] selected = batch.selectedInUse ? batch.selected : null;
    for (int k = 0; k < batch.size; k++) {
      int i = selected == null ? k : selected[k];
      hashes[i] = hashRow(batch, i);
    }
  }

  int hashRow(VectorizedRowBatch batch, int i) {
    hash = 0;
    word = 0;
    wordBytes = 0;
    length = 0;
    for (int c = 0; c < columns.length; c++) {
      LongColumnVector column = (LongColumnVector) batch.cols[columns[c]];
      int row = column.isRepeating ? 0 : i;
      int invert = descending[c] ? 0xff : 0;
      if (!column.noNulls && column.isNull[row]) {
        add((byte) (nullMarkers[c] ^ invert));
        continue;
      }
      add((byte) (notNullMarkers[c] ^ invert));
      long v = column.vector[row];
      int width = widths[c];
      if (width == 0) {
        add((byte) ((v != 0 ? 2 : 1) ^ invert));
        continue;
      }
      // the most significant byte first, its sign bit flipped
      for (int b = width - 1; b >= 0; b--) {
        int value = (int) (v >> (8 * b));
        if (b == width - 1) {
          value ^= 0x80;
        }
        add((byte) (value ^ invert));
      }
    }
    return finish();
  }

  private void add(byte b) {
    if (wordBytes < 3) {
      tail[wordBytes] = b;
    }
    word |= (b & 0xff) << (8 * wordBytes);
    length++;
    if (++wordBytes == 4) {
      int k = word * C1;
      k = Integer.rotateLeft(k, 15) * C2;
      hash ^= k;
      hash = Integer.rotateLeft(hash, 13) * 5 + 0xe6546b64;
      word = 0;
      wordBytes = 0;
    }
  }

  private int finish() {
    if (wordBytes > 0) {
      int k1 = 0;
      switch (wordBytes) {
      case 3:
        k1 ^= tail[2] << 16;
      case 2:
        k1 ^= tail[1] << 8;
      case 1:
        k1 ^= tail[0];
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        hash ^= k1;
      default:
        break;
      }
    }
    int h = hash ^ length;
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }
}
