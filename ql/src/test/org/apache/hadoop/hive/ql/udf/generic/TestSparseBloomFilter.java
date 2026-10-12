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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.hive.common.util.BloomKFilter;
import org.junit.Assert;
import org.junit.Test;

public class TestSparseBloomFilter {
  private static final long EXPECTED_ENTRIES = 1_000_000;

  @Test
  public void testSameBitsAsTheBloomFilter() throws Exception {
    BloomKFilter expected = BloomKFilter.build(EXPECTED_ENTRIES);
    SparseBloomFilter sparse = new SparseBloomFilter(EXPECTED_ENTRIES);
    for (int i = 0; i < 5000; i++) {
      byte[] value = ("value" + i).getBytes(StandardCharsets.UTF_8);
      expected.addLong(i);
      expected.addDouble(i + 0.5);
      expected.addBytes(value);
      Assert.assertTrue(sparse.add(BloomKFilter.hashLong(i)));
      Assert.assertTrue(sparse.add(BloomKFilter.hashDouble(i + 0.5)));
      Assert.assertTrue(sparse.add(BloomKFilter.hashBytes(value, 0, value.length)));
    }
    expected.addBytes(null, 0, 0);
    Assert.assertTrue(sparse.add(BloomKFilter.hashBytes(null, 0, 0)));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    sparse.serialize(out);
    byte[] bytes = out.toByteArray();
    Assert.assertTrue(SparseBloomFilter.isSparse(bytes, 0, bytes.length));
    BloomKFilter merged = BloomKFilter.build(EXPECTED_ENTRIES);
    SparseBloomFilter.addTo(merged, bytes, 0, bytes.length);
    Assert.assertArrayEquals(expected.getBitSet(), merged.getBitSet());

    ByteArrayOutputStream dense = new ByteArrayOutputStream();
    BloomKFilter.serialize(dense, expected);
    Assert.assertFalse(SparseBloomFilter.isSparse(dense.toByteArray(), 0, dense.size()));
  }

  @Test
  public void testDuplicatesStaySparse() throws Exception {
    SparseBloomFilter sparse = new SparseBloomFilter(EXPECTED_ENTRIES);
    int maxHashes = BloomKFilter.getNumLongs(EXPECTED_ENTRIES);
    for (int i = 0; i < 10 * maxHashes; i++) {
      Assert.assertTrue(sparse.add(BloomKFilter.hashLong(i % 100)));
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    sparse.serialize(out);
    Assert.assertEquals(1 + 4 + 100 * Long.BYTES, out.size());
  }

  @Test
  public void testOutgrowsTheBitSet() {
    SparseBloomFilter sparse = new SparseBloomFilter(EXPECTED_ENTRIES);
    int maxHashes = BloomKFilter.getNumLongs(EXPECTED_ENTRIES);
    int added = 0;
    while (sparse.add(BloomKFilter.hashLong(added))) {
      added++;
    }
    Assert.assertTrue(added > maxHashes / 2);
    Assert.assertTrue(added <= maxHashes);
  }
}
