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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.stream.LongStream;

import org.apache.hadoop.hive.common.NoDynamicValuesException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColumnBetweenDynamicValue;
import org.apache.hadoop.hive.ql.plan.DynamicValue;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hive.common.util.BloomKFilter;
import org.junit.Test;

/** The runtime filters pass the rows while their values have not arrived, and filter once they have. */
public class TestDynamicValueFilters {

  private static final long[] KEYS = LongStream.rangeClosed(1, 10).toArray();

  @Test
  public void testBetweenPassesRowsUntilItsBoundsArrive() throws Exception {
    ArrivingValue min = new ArrivingValue("min", TypeInfoFactory.longTypeInfo, new LongWritable(3));
    ArrivingValue max = new ArrivingValue("max", TypeInfoFactory.longTypeInfo, new LongWritable(5));
    FilterLongColumnBetweenDynamicValue filter = new FilterLongColumnBetweenDynamicValue(0, min, max);
    filter.setInputTypeInfos(TypeInfoFactory.longTypeInfo);
    HiveConf conf = new HiveConf();
    filter.transientInit(conf);
    filter.init(conf);

    assertArrayEquals(KEYS, selected(filter, batch()));

    min.arrive();
    max.arrive();
    assertArrayEquals(new long[] {3, 4, 5}, selected(filter, batch()));
  }

  @Test
  public void testBloomFilterPassesRowsUntilItArrives() throws Exception {
    BloomKFilter bloomFilter = BloomKFilter.build(3);
    LongStream.rangeClosed(3, 5).forEach(bloomFilter::addLong);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    BloomKFilter.serialize(bytes, bloomFilter);
    ArrivingValue value = new ArrivingValue("bloom", TypeInfoFactory.binaryTypeInfo,
        new BytesWritable(bytes.toByteArray()));
    VectorInBloomFilterColDynamicValue filter = new VectorInBloomFilterColDynamicValue(0, value);
    filter.setInputTypeInfos(TypeInfoFactory.longTypeInfo);
    HiveConf conf = new HiveConf();
    // outside Tez and LLAP the filter's deserialization cache is the plain one
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
    filter.transientInit(conf);
    filter.init(conf);

    assertArrayEquals(KEYS, selected(filter, batch()));

    value.arrive();
    long[] expected = Arrays.stream(KEYS).filter(bloomFilter::testLong).toArray();
    assertFalse(Arrays.equals(KEYS, expected));
    assertArrayEquals(expected, selected(filter, batch()));
  }

  private static VectorizedRowBatch batch() {
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    LongColumnVector keys = new LongColumnVector();
    System.arraycopy(KEYS, 0, keys.vector, 0, KEYS.length);
    batch.cols[0] = keys;
    batch.size = KEYS.length;
    return batch;
  }

  /** The keys of the rows the filter selects. */
  private static long[] selected(VectorExpression filter, VectorizedRowBatch batch) throws Exception {
    filter.evaluate(batch);
    long[] keys = ((LongColumnVector) batch.cols[0]).vector;
    long[] selected = new long[batch.size];
    for (int i = 0; i < batch.size; i++) {
      selected[i] = keys[batch.selectedInUse ? batch.selected[i] : i];
    }
    return selected;
  }

  /** A runtime value that is missing until it arrives. */
  private static final class ArrivingValue extends DynamicValue {
    private final Object value;
    private boolean arrived;

    ArrivingValue(String id, TypeInfo typeInfo, Object value) {
      super(id, typeInfo);
      this.value = value;
    }

    void arrive() {
      arrived = true;
    }

    @Override
    public Object getValue() {
      if (!arrived) {
        throw new NoDynamicValuesException("not arrived: " + getId());
      }
      return value;
    }
  }
}
