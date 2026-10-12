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
package org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.udf.generic.SparseBloomFilter;
import org.apache.hive.common.util.BloomKFilter;
import org.junit.Assert;
import org.junit.Test;

public class TestVectorUDAFBloomFilterMerge {

  @Test
  public void testMergeBloomKFilterBytesParallel() throws Exception {
    testMergeBloomKFilterBytesParallel(1);
    testMergeBloomKFilterBytesParallel(2);
    testMergeBloomKFilterBytesParallel(4);
    testMergeBloomKFilterBytesParallel(8);
  }

  private void testMergeBloomKFilterBytesParallel(int threads) throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.TEZ_BLOOM_FILTER_MERGE_THREADS.varname, threads);

    int expectedEntries = 1000000;
    byte[] bf1Bytes = getBloomKFilterBytesFromStringValues(expectedEntries, "bloo", "bloom fil",
        "bloom filter", "cuckoo filter");
    byte[] bf2Bytes = getBloomKFilterBytesFromStringValues(expectedEntries, "2_bloo", "2_bloom fil",
        "2_bloom filter", "2_cuckoo filter");
    byte[] bf3Bytes = getBloomKFilterBytesFromStringValues(expectedEntries, "3_bloo", "3_bloom fil",
        "3_bloom filter", "3_cuckoo filter");
    byte[] bf4Bytes = getBloomKFilterBytesFromStringValues(expectedEntries, "4_bloo", "4_bloom fil",
        "4_bloom filter", "4_cuckoo filter");
    byte[] bf5Bytes = getBloomKFilterBytesFromStringValues(expectedEntries, "5_bloo", "5_bloom fil",
        "5_bloom filter", "5_cuckoo filter");

    BytesColumnVector columnVector = new BytesColumnVector();
    columnVector.reset(); // init buffers
    columnVector.setVal(0, bf1Bytes);
    columnVector.setVal(1, bf2Bytes);
    columnVector.setVal(2, bf3Bytes);

    BytesColumnVector columnVector2 = new BytesColumnVector();
    columnVector2.reset(); // init buffers
    columnVector2.setVal(0, bf4Bytes);
    columnVector2.setVal(1, bf5Bytes);

    VectorUDAFBloomFilterMerge.Aggregation agg =
        new VectorUDAFBloomFilterMerge.Aggregation(expectedEntries, threads);
    agg.mergeBloomFilterBytesFromInputColumn(columnVector, 1024, false, null);
    agg.mergeBloomFilterBytesFromInputColumn(columnVector2, 1024, false, null);
    new VectorUDAFBloomFilterMerge().finish(agg, false);

    BloomKFilter merged = BloomKFilter.deserialize(new ByteArrayInputStream(agg.bfBytes));
    Assert.assertTrue(merged.testBytes("bloo".getBytes()));
    Assert.assertTrue(merged.testBytes("cuckoo filter".getBytes()));
    Assert.assertTrue(merged.testBytes("2_bloo".getBytes()));
    Assert.assertTrue(merged.testBytes("2_cuckoo filter".getBytes()));
    Assert.assertTrue(merged.testBytes("3_bloo".getBytes()));
    Assert.assertTrue(merged.testBytes("3_cuckoo filter".getBytes()));
    Assert.assertTrue(merged.testBytes("4_bloo".getBytes()));
    Assert.assertTrue(merged.testBytes("4_cuckoo filter".getBytes()));
    Assert.assertTrue(merged.testBytes("5_bloo".getBytes()));
    Assert.assertTrue(merged.testBytes("5_cuckoo filter".getBytes()));
  }

  @Test
  public void testMergeSparseAndDense() throws Exception {
    for (int threads = 0; threads <= 8; threads = threads == 0 ? 1 : threads * 2) {
      int expectedEntries = 1000000;
      BloomKFilter expected = BloomKFilter.build(expectedEntries);
      BytesColumnVector columnVector = new BytesColumnVector();
      columnVector.reset();
      for (int p = 0; p < 4; p++) {
        BloomKFilter dense = BloomKFilter.build(expectedEntries);
        SparseBloomFilter sparse = new SparseBloomFilter(expectedEntries);
        for (long v = 0; v < 1000; v++) {
          long value = p * 1000 + v;
          expected.addLong(value);
          if (p % 2 == 0) {
            dense.addLong(value);
          } else {
            Assert.assertTrue(sparse.add(BloomKFilter.hashLong(value)));
          }
        }
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        if (p % 2 == 0) {
          BloomKFilter.serialize(bytesOut, dense);
        } else {
          sparse.serialize(bytesOut);
        }
        columnVector.setVal(p, bytesOut.toByteArray());
      }

      VectorUDAFBloomFilterMerge.Aggregation agg = new VectorUDAFBloomFilterMerge.Aggregation(expectedEntries, threads);
      if (threads == 0) {
        VectorUDAFBloomFilterMerge merge = new VectorUDAFBloomFilterMerge();
        for (int p = 0; p < 4; p++) {
          merge.processValue(agg, columnVector, p);
        }
      } else {
        agg.mergeBloomFilterBytesFromInputColumn(columnVector, 4, false, null);
        new VectorUDAFBloomFilterMerge().finish(agg, false);
      }
      ByteArrayOutputStream expectedBytes = new ByteArrayOutputStream();
      BloomKFilter.serialize(expectedBytes, expected);
      Assert.assertTrue("threads " + threads, Arrays.equals(expectedBytes.toByteArray(), agg.result()));
    }
  }

  private byte[] getBloomKFilterBytesFromStringValues(int expectedEntries, String... values)
      throws IOException {
    BloomKFilter bf = BloomKFilter.build(expectedEntries);
    for (String val : values) {
      bf.addString(val);
    }

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    BloomKFilter.serialize(bytesOut, bf);
    return bytesOut.toByteArray();
  }
}
