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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hive.common.util.Murmur3;
import org.junit.Test;

public class TestBinarySortableKeyHash {

  /**
   * For random keys of every fixed width, nulls, ascending and descending columns with either null order, the hash
   * equals Murmur3.hash32 of the bytes BinarySortableSerializeWrite writes for the row.
   */
  @Test
  public void testSameHashAsTheSerializedKey() throws Exception {
    Random random = new Random(3);
    int[] widthChoices = {0, 1, 2, 4, 8};
    for (int round = 0; round < 200; round++) {
      int keys = 1 + random.nextInt(4);
      int[] widths = new int[keys];
      int[] columns = new int[keys];
      boolean[] desc = new boolean[keys];
      byte[] nullMarkers = new byte[keys];
      byte[] notNullMarkers = new byte[keys];
      VectorizedRowBatch batch = new VectorizedRowBatch(keys);
      for (int c = 0; c < keys; c++) {
        widths[c] = widthChoices[random.nextInt(widthChoices.length)];
        columns[c] = c;
        desc[c] = random.nextBoolean();
        // BinarySortable's markers: nulls first is (0, 1), nulls last is (2, 1)
        nullMarkers[c] = random.nextBoolean() ? (byte) 0 : (byte) 2;
        notNullMarkers[c] = 1;
        LongColumnVector column = new LongColumnVector();
        column.isRepeating = random.nextInt(6) == 0;
        for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
          if (random.nextInt(5) == 0) {
            column.noNulls = false;
            column.isNull[i] = true;
          }
          column.vector[i] = value(widths[c], random);
        }
        batch.cols[c] = column;
      }
      batch.size = VectorizedRowBatch.DEFAULT_SIZE;
      BinarySortableKeyHash hasher = new BinarySortableKeyHash(columns, widths, nullMarkers, notNullMarkers, desc);
      int[] hashes = new int[VectorizedRowBatch.DEFAULT_SIZE];
      hasher.hash(batch, hashes);

      BinarySortableSerializeWrite write = new BinarySortableSerializeWrite(desc, nullMarkers, notNullMarkers);
      Output output = new Output();
      for (int i = 0; i < batch.size; i++) {
        write.set(output);
        for (int c = 0; c < keys; c++) {
          LongColumnVector column = (LongColumnVector) batch.cols[c];
          int row = column.isRepeating ? 0 : i;
          if (!column.noNulls && column.isNull[row]) {
            write.writeNull();
            continue;
          }
          long v = column.vector[row];
          switch (widths[c]) {
          case 0 -> write.writeBoolean(v != 0);
          case 1 -> write.writeByte((byte) v);
          case 2 -> write.writeShort((short) v);
          case 4 -> write.writeInt((int) v);
          default -> write.writeLong(v);
          }
        }
        assertEquals("round " + round + " row " + i, Murmur3.hash32(output.getData(), 0, output.getLength(), 0),
            hashes[i]);
      }
    }
  }

  private static long value(int width, Random random) {
    return switch (width) {
      case 0 -> random.nextInt(2);
      case 1 -> (byte) random.nextInt();
      case 2 -> (short) random.nextInt();
      case 4 -> random.nextInt();
      default -> random.nextLong();
    };
  }
}
