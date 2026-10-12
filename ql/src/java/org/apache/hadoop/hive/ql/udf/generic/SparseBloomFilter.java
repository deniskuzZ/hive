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

package org.apache.hadoop.hive.ql.udf.generic;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hive.common.util.BloomKFilter;

/**
 * A partial bloom filter kept as the distinct hashes of its values while they take less space than the bit set
 * (8 bytes per hash): a map task of a large semijoin bloom filter adds a small share of its entries, yet a bit set
 * is as large as the whole filter. The merge adds the hashes to its bloom filter.
 *
 * <p>Serialized as a 0 byte (a serialized {@link BloomKFilter} starts with its number of hash functions, at least
 * 1), the number of hashes and the hashes.
 */
public final class SparseBloomFilter {
  private static final byte MARKER = 0;
  private static final int INITIAL_CAPACITY = 1024;

  private final int maxHashes;
  private long[] hashes;
  private int size;

  public SparseBloomFilter(long expectedEntries) {
    maxHashes = BloomKFilter.getNumLongs(expectedEntries);
    hashes = new long[Math.min(INITIAL_CAPACITY, maxHashes)];
  }

  /**
   * Adds a hash ({@link BloomKFilter#hashLong} and friends); returns false, without adding it, once the distinct
   * hashes outgrow half of the bit set.
   */
  public boolean add(long hash) {
    if (size == hashes.length && !makeRoom()) {
      return false;
    }
    hashes[size++] = hash;
    return true;
  }

  private boolean makeRoom() {
    distinct();
    if (size <= hashes.length / 2) {
      return true;
    }
    if (hashes.length == maxHashes) {
      return false;
    }
    hashes = Arrays.copyOf(hashes, (int) Math.min(2L * hashes.length, maxHashes));
    return true;
  }

  private void distinct() {
    Arrays.sort(hashes, 0, size);
    int distinct = 0;
    for (int i = 0; i < size; i++) {
      if (distinct == 0 || hashes[i] != hashes[distinct - 1]) {
        hashes[distinct++] = hashes[i];
      }
    }
    size = distinct;
  }

  public void addTo(BloomKFilter bloomFilter) {
    for (int i = 0; i < size; i++) {
      bloomFilter.addHash(hashes[i]);
    }
  }

  public void serialize(OutputStream out) throws IOException {
    distinct();
    DataOutputStream dataOut = new DataOutputStream(out);
    dataOut.writeByte(MARKER);
    dataOut.writeInt(size);
    for (int i = 0; i < size; i++) {
      dataOut.writeLong(hashes[i]);
    }
    dataOut.flush();
  }

  public static boolean isSparse(byte[] bytes, int start, int length) {
    return length > 0 && bytes[start] == MARKER;
  }

  /**
   * Adds the hashes of a serialized sparse bloom filter to a bloom filter.
   */
  public static void addTo(BloomKFilter bloomFilter, byte[] bytes, int start, int length) {
    ByteBuffer in = ByteBuffer.wrap(bytes, start + 1, length - 1);
    for (int i = in.getInt(); i > 0; i--) {
      bloomFilter.addHash(in.getLong());
    }
  }
}
