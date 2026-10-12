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

import java.io.ByteArrayOutputStream;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.common.util.BloomKFilter;
import org.junit.Assert;
import org.junit.Test;

public class TestVectorUDAFBloomFilter {
  private static final int EXPECTED_ENTRIES = 1000;

  /**
   * The map-side bloom filter holds the hashes sparsely until they outgrow the bit set. Below and above that, a
   * partial result merges into, and a complete one equals, the dense filter of the same values; a reset buffer starts
   * over.
   */
  @Test
  public void testSparseAndDense() throws Exception {
    int sparseCapacity = BloomKFilter.getNumLongs(EXPECTED_ENTRIES);
    for (int distinct : new int[] { 10, sparseCapacity + 10, 50 * sparseCapacity }) {
      BloomKFilter expected = BloomKFilter.build(EXPECTED_ENTRIES);
      for (long v = 0; v < distinct; v++) {
        expected.addLong(v);
      }
      ByteArrayOutputStream expectedBytes = new ByteArrayOutputStream();
      BloomKFilter.serialize(expectedBytes, expected);
      for (Mode mode : new Mode[] { Mode.PARTIAL1, Mode.COMPLETE }) {
        VectorUDAFBloomFilter udaf = udaf(mode);
        AggregationBuffer agg = udaf.getNewAggregationBuffer();
        for (int round = 0; round < 2; round++) {
          udaf.reset(agg);
          aggregate(udaf, agg, distinct);
          byte[] result = result(udaf, agg);
          if (mode == Mode.PARTIAL1) {
            result = merge(result);
          }
          Assert.assertArrayEquals(mode + ", " + distinct + " values", expectedBytes.toByteArray(), result);
        }
      }
    }
  }

  private static VectorUDAFBloomFilter udaf(Mode mode) {
    GenericUDAFBloomFilterEvaluator evaluator = new GenericUDAFBloomFilterEvaluator();
    evaluator.setHintEntries(EXPECTED_ENTRIES);
    IdentityExpression input = new IdentityExpression(0);
    input.setOutputTypeInfo(TypeInfoFactory.longTypeInfo);
    input.setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.NONE);
    return new VectorUDAFBloomFilter(new VectorAggregationDesc.VectorAggregationDescBuilder()
        .aggregationName("bloom_filter")
        .evaluator(evaluator)
        .udafEvaluatorMode(mode)
        .inputTypeInfo(TypeInfoFactory.longTypeInfo)
        .inputColVectorType(ColumnVector.Type.LONG)
        .inputExpression(input)
        .outputTypeInfo(TypeInfoFactory.binaryTypeInfo)
        .outputColVectorType(ColumnVector.Type.BYTES)
        .vectorAggregationClass(VectorUDAFBloomFilter.class)
        .build());
  }

  // every value once, then the first values again, in batches that cut across the values: a value that overflows
  // the sparse hashes is not added again later
  private static void aggregate(VectorUDAFBloomFilter udaf, AggregationBuffer agg, int distinct) throws Exception {
    int repeated = Math.min(5, distinct);
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    LongColumnVector column = new LongColumnVector();
    batch.cols[0] = column;
    for (int start = 0; start < distinct + repeated; start += batch.getMaxSize()) {
      batch.size = Math.min(batch.getMaxSize(), distinct + repeated - start);
      for (int i = 0; i < batch.size; i++) {
        column.vector[i] = (start + i) % distinct;
      }
      udaf.aggregateInput(agg, batch);
    }
  }

  private static byte[] result(VectorUDAFBloomFilter udaf, AggregationBuffer agg) throws Exception {
    VectorizedRowBatch output = new VectorizedRowBatch(1);
    BytesColumnVector column = new BytesColumnVector(1);
    column.initBuffer();
    output.cols[0] = column;
    udaf.assignRowColumn(output, 0, 0, agg);
    byte[] bytes = new byte[column.length[0]];
    System.arraycopy(column.vector[0], column.start[0], bytes, 0, bytes.length);
    return bytes;
  }

  private static byte[] merge(byte[] partial) throws Exception {
    BytesColumnVector column = new BytesColumnVector(1);
    column.initBuffer();
    column.setVal(0, partial);
    VectorUDAFBloomFilterMerge.Aggregation agg = new VectorUDAFBloomFilterMerge.Aggregation(EXPECTED_ENTRIES, 0);
    new VectorUDAFBloomFilterMerge().processValue(agg, column, 0);
    return agg.result();
  }
}
