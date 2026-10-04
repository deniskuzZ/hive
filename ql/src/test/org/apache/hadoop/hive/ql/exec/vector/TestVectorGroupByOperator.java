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

package org.apache.hadoop.hive.ql.exec.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFBloomFilterMerge;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCount;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCountStar;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeCaptureOutputOperator;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeCaptureVectorToRowOutputOperator;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeVectorRowBatchFromConcat;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeVectorRowBatchFromLongIterables;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeVectorRowBatchFromObjectIterables;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeVectorRowBatchFromRepeats;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc.ProcessingMode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBloomFilter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStdSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVarianceSample;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for the vectorized GROUP BY operator.
 */
public class TestVectorGroupByOperator {

  HiveConf hconf = new HiveConf();

  private static ExprNodeDesc buildColumnDesc(
      VectorizationContext ctx,
      String column,
      TypeInfo typeInfo) {

    return new ExprNodeColumnDesc(
        typeInfo, column, "table", false);
  }

  private static AggregationDesc buildAggregationDesc(
      VectorizationContext ctx,
      String aggregate,
      GenericUDAFEvaluator.Mode mode,
      String column,
      TypeInfo typeInfo) {


    TypeInfo[] typeInfos = new TypeInfo[] {typeInfo};
    ArrayList<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>(1);
    ExprNodeDesc inputColumn = buildColumnDesc(ctx, column, typeInfo);
    params.add(inputColumn);

    AggregationDesc agg = new AggregationDesc();
    agg.setGenericUDAFName(aggregate);
    agg.setMode(mode);
    agg.setParameters(params);

    final GenericUDAFEvaluator evaluator;
    try {
      switch (aggregate) {
      case "count":
        evaluator = new GenericUDAFCount.GenericUDAFCountEvaluator();
        break;
      case "min":
        evaluator = new GenericUDAFMin.GenericUDAFMinEvaluator();
        break;
      case "max":
        evaluator = new GenericUDAFMax.GenericUDAFMaxEvaluator();
        break;
      case "sum":
        evaluator = (new GenericUDAFSum()).getEvaluator(typeInfos);
        break;
      case "avg":
        evaluator = (new GenericUDAFAverage()).getEvaluator(typeInfos);
        break;
      case "variance":
      case "var":
      case "var_pop":
        evaluator = new GenericUDAFVariance.GenericUDAFVarianceEvaluator();
        break;
      case "var_samp":
        evaluator = new GenericUDAFVarianceSample.GenericUDAFVarianceSampleEvaluator();
        break;
      case "std":
      case "stddev":
      case "stddev_pop":
        evaluator = new GenericUDAFStd.GenericUDAFStdEvaluator();
        break;
      case "stddev_samp":
        evaluator = new GenericUDAFStdSample.GenericUDAFStdSampleEvaluator();
        break;
      default:
        throw new RuntimeException("Unexpected aggregate " + aggregate);
      }
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
    agg.setGenericUDAFEvaluator(evaluator);
    return agg;
  }
  private static AggregationDesc buildAggregationDescCountStar(
      VectorizationContext ctx) {
    AggregationDesc agg = new AggregationDesc();
    agg.setGenericUDAFName("count");
    agg.setMode(GenericUDAFEvaluator.Mode.PARTIAL1);
    agg.setParameters(new ArrayList<ExprNodeDesc>());
    agg.setGenericUDAFEvaluator(new GenericUDAFCount.GenericUDAFCountEvaluator());
    return agg;
  }


  private static Pair<GroupByDesc,VectorGroupByDesc> buildGroupByDescType(
      VectorizationContext ctx,
      String aggregate,
      GenericUDAFEvaluator.Mode mode,
      String column,
      TypeInfo dataType) {

    AggregationDesc agg = buildAggregationDesc(ctx, aggregate, mode,
        column, dataType);
    ArrayList<AggregationDesc> aggs = new ArrayList<AggregationDesc>();
    aggs.add(agg);

    ArrayList<String> outputColumnNames = new ArrayList<String>();
    outputColumnNames.add("_col0");

    GroupByDesc desc = new GroupByDesc();
    VectorGroupByDesc vectorDesc = new VectorGroupByDesc();

    desc.setOutputColumnNames(outputColumnNames);
    desc.setAggregators(aggs);
    vectorDesc.setProcessingMode(ProcessingMode.GLOBAL);

    return new Pair<GroupByDesc,VectorGroupByDesc>(desc, vectorDesc);
  }

  private static Pair<GroupByDesc,VectorGroupByDesc> buildGroupByDescCountStar(
      VectorizationContext ctx) {

    AggregationDesc agg = buildAggregationDescCountStar(ctx);
    ArrayList<AggregationDesc> aggs = new ArrayList<AggregationDesc>();
    aggs.add(agg);

    ArrayList<String> outputColumnNames = new ArrayList<String>();
    outputColumnNames.add("_col0");

    GroupByDesc desc = new GroupByDesc();
    VectorGroupByDesc vectorDesc = new VectorGroupByDesc();
    vectorDesc.setVecAggrDescs(
        new VectorAggregationDesc[] {
            new VectorAggregationDesc.VectorAggregationDescBuilder()
                .aggregationName(agg.getGenericUDAFName())
                .evaluator(new GenericUDAFCount.GenericUDAFCountEvaluator())
                .udafEvaluatorMode(agg.getMode())
                .inputColVectorType(ColumnVector.Type.NONE)
                .outputTypeInfo(TypeInfoFactory.longTypeInfo)
                .outputColVectorType(ColumnVector.Type.LONG)
                .vectorAggregationClass(VectorUDAFCountStar.class)
                .build()});
    vectorDesc.setProcessingMode(VectorGroupByDesc.ProcessingMode.HASH);

    desc.setOutputColumnNames(outputColumnNames);
    desc.setAggregators(aggs);

    return new Pair<GroupByDesc,VectorGroupByDesc>(desc, vectorDesc);
  }

  private static Pair<GroupByDesc,VectorGroupByDesc> buildKeyGroupByDesc(
      VectorizationContext ctx,
      String aggregate,
      String column,
      TypeInfo dataTypeInfo,
      String[] keys,
      TypeInfo[] keyTypeInfos) {

    Pair<GroupByDesc,VectorGroupByDesc> pair =
        buildGroupByDescType(ctx, aggregate, GenericUDAFEvaluator.Mode.PARTIAL1, column, dataTypeInfo);
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;
    vectorDesc.setProcessingMode(ProcessingMode.HASH);

    ArrayList<ExprNodeDesc> keyExprs = new ArrayList<ExprNodeDesc>(keys.length);
    for (int i = 0; i < keys.length; i++) {
      final String key = keys[i];
      final TypeInfo keyTypeInfo = keyTypeInfos[i];
      final ExprNodeDesc keyExp = buildColumnDesc(ctx, key, keyTypeInfo);
      keyExprs.add(keyExp);
    }
    desc.setKeys(keyExprs);

    desc.getOutputColumnNames().add("_col1");

    return pair;
  }

  long outputRowCount = 0;

  @Test
  public void testMemoryPressureFlush() throws HiveException {

    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("Key");
    mapColumnNames.add("Value");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildKeyGroupByDesc (ctx, "max",
        "Value", TypeInfoFactory.longTypeInfo,
        new String[] {"Key"},
        new TypeInfo[] {TypeInfoFactory.longTypeInfo});
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;

    // Set the memory threshold so that we get 100Kb before we need to flush.
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    long maxMemory = memoryMXBean.getHeapMemoryUsage().getMax();

    float threshold = 100.0f*1024.0f/maxMemory;
    desc.setGroupByMemoryUsage(threshold);

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    long expected = vgo.getMaxMemory();
    assertEquals(expected, maxMemory);
    this.outputRowCount = 0;
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {
      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        ++outputRowCount;
      }
    });

    Iterable<Object> it = new Iterable<Object>() {
      @Override
      public Iterator<Object> iterator() {
        return new Iterator<Object> () {
          long value = 0;

          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public Object next() {
            return ++value;
          }

          @Override
          public void remove() {
          }
        };
      }
    };

    FakeVectorRowBatchFromObjectIterables data = new FakeVectorRowBatchFromObjectIterables(
        100,
        new String[] {"long", "long"},
        it,
        it);

    // The 'it' data source will produce data w/o ever ending
    // We want to see that memory pressure kicks in and some
    // entries in the VGBY are flushed.
    long countRowsProduced = 0;
    for (VectorizedRowBatch unit: data) {
      countRowsProduced += 100;
      vgo.process(unit,  0);
      if (0 < outputRowCount) {
        break;
      }
      // Set an upper bound how much we're willing to push before it should flush
      // we've set the memory threshold at 100kb, each key is distinct
      // It should not go beyond 100k/16 (key+data)
      assertTrue(countRowsProduced < 100*1024/16);
    }

    assertTrue(0 < outputRowCount);
  }

  @Test
  public void testMemoryPressureFlushLlap() throws HiveException {

    try {
      List<String> mapColumnNames = new ArrayList<String>();
      mapColumnNames.add("Key");
      mapColumnNames.add("Value");
      VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

      Pair<GroupByDesc, VectorGroupByDesc> pair = buildKeyGroupByDesc(ctx, "max",
        "Value", TypeInfoFactory.longTypeInfo,
        new String[] {"Key"},
        new TypeInfo[] {TypeInfoFactory.longTypeInfo});
      GroupByDesc desc = pair.left;
      VectorGroupByDesc vectorDesc = pair.right;

      LlapProxy.setDaemon(true);

      CompilationOpContext cCtx = new CompilationOpContext();

      Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

      VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

      FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
      long maxMemory=512*1024*1024L;
      vgo.getConf().setMaxMemoryAvailable(maxMemory);
      float threshold = 100.0f*1024.0f/maxMemory;
      desc.setGroupByMemoryUsage(threshold);
      vgo.initialize(hconf, null);

      long got = vgo.getMaxMemory();
      assertEquals(maxMemory, got);
      this.outputRowCount = 0;
      out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {
        @Override
        public void inspectRow(Object row, int tag) throws HiveException {
          ++outputRowCount;
        }
      });

      Iterable<Object> it = new Iterable<Object>() {
        @Override
        public Iterator<Object> iterator() {
          return new Iterator<Object>() {
            long value = 0;

            @Override
            public boolean hasNext() {
              return true;
            }

            @Override
            public Object next() {
              return ++value;
            }

            @Override
            public void remove() {
            }
          };
        }
      };

      FakeVectorRowBatchFromObjectIterables data = new FakeVectorRowBatchFromObjectIterables(
        100,
        new String[]{"long", "long"},
        it,
        it);

      // The 'it' data source will produce data w/o ever ending
      // We want to see that memory pressure kicks in and some
      // entries in the VGBY are flushed.
      long countRowsProduced = 0;
      for (VectorizedRowBatch unit : data) {
        countRowsProduced += 100;
        vgo.process(unit, 0);
        if (0 < outputRowCount) {
          break;
        }
        // Set an upper bound how much we're willing to push before it should flush
        // we've set the memory threshold at 100kb, each key is distinct
        // It should not go beyond 100k/16 (key+data)
        assertTrue(countRowsProduced < 100 * 1024 / 16);
      }

      assertTrue(0 < outputRowCount);
    } finally {
      LlapProxy.setDaemon(false);
    }
  }

  @Test
  public void testRollupAggregation() throws HiveException {

    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("k1");
    mapColumnNames.add("k2");
    mapColumnNames.add("v");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    // select count(v) from name group by rollup (k1,k2);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildKeyGroupByDesc (ctx, "count",
        "v", TypeInfoFactory.longTypeInfo,
        new String[] { "k1", "k2" },
        new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo});
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;

    desc.setGroupingSetsPresent(true);
    ArrayList<Long> groupingSets = new ArrayList<>();
    // groupingSets
    groupingSets.add(0L);
    groupingSets.add(1L);
    groupingSets.add(2L);
    desc.setListGroupingSets(groupingSets);
    // add grouping sets dummy key
    ExprNodeDesc groupingSetDummyKey = new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 0L);
    // this only works because we used an arraylist in buildKeyGroupByDesc
    // don't do this in actual compiler
    desc.getKeys().add(groupingSetDummyKey);
    // groupingSet Position
    desc.setGroupingSetPosition(2);

    CompilationOpContext cCtx = new CompilationOpContext();

    desc.setMinReductionHashAggr(0.5f);
    // Set really low check interval setting
    hconf.set("hive.groupby.mapaggr.checkinterval", "10");
    hconf.set("hive.vectorized.groupby.checkinterval", "10");

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    this.outputRowCount = 0;
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {
      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        ++outputRowCount;
      }
    });

    // vrb of 1 row each
    FakeVectorRowBatchFromObjectIterables data = getDataForRollup();

    long countRowsProduced = 0;
    for (VectorizedRowBatch unit: data) {
      // after 24 rows, we'd have seen all the keys
      // find 14 keys in the hashmap
      // but 24*0.5 = 12
      // won't turn off hash mode because of the 3 grouping sets
      // if it turns off the hash mode, we'd get 14 + 3*(100-24) rows
      countRowsProduced += unit.size;
      vgo.process(unit,  0);

      if (countRowsProduced >= 100) {
        break;
      }

    }
    vgo.close(false);
    // all groupings
    // 10 keys generates 14 rows with the rollup
    assertEquals(1+3+10, outputRowCount);
  }

  FakeVectorRowBatchFromObjectIterables getDataForRollup() throws HiveException {
    // k1 has nDV of 2
    Iterable<Object> k1 = new Iterable<Object>() {
      @Override
      public Iterator<Object> iterator() {
        return new Iterator<Object>() {
          int value = 0;
          int ndv = 2;

          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public Object next() {
            value = (value + 1) % ndv;
            return value;
          }

          @Override
          public void remove() {
          }
        };
      }
    };

    // ndv of 5
    Iterable<Object> k2 = new Iterable<Object>() {
      @Override
      public Iterator<Object> iterator() {
        return new Iterator<Object>() {
          int value = 0;
          int ndv = 6;

          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public Object next() {
            value = (value + 1) % ndv;
            return value;
          }

          @Override
          public void remove() {
          }
        };
      }
    };

    // just return 1, we're running "count"
    Iterable<Object> v = new Iterable<Object>() {
      @Override
      public Iterator<Object> iterator() {
        return new Iterator<Object>() {
          int value = 0;
          int ndv = 1;

          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public Object next() {
            value = (value + 1) % ndv;
            return value;
          }

          @Override
          public void remove() {
          }
        };
      }
    };

    // vrb of 1 row each
    return new FakeVectorRowBatchFromObjectIterables(
        2,
        new String[] {"long", "long", "long", "long"},
        k1,
        k2,
        v,
        v); // output col
  }

  @Test
  public void testRollupAggregationWithBufferReuse() throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("k1");
    mapColumnNames.add("k2");
    mapColumnNames.add("v");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    // select count(v) from name group by rollup (k1,k2);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildKeyGroupByDesc (ctx, "count",
        "v", TypeInfoFactory.longTypeInfo,
        new String[] { "k1", "k2" },
        new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo});
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;

    desc.setGroupingSetsPresent(true);
    ArrayList<Long> groupingSets = new ArrayList<>();
    // groupingSets
    groupingSets.add(0L);
    groupingSets.add(1L);
    groupingSets.add(2L);
    desc.setListGroupingSets(groupingSets);
    // add grouping sets dummy key
    ExprNodeDesc groupingSetDummyKey = new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 0L);

    desc.getKeys().add(groupingSetDummyKey);
    // groupingSet Position
    desc.setGroupingSetPosition(2);

    CompilationOpContext cCtx = new CompilationOpContext();

    desc.setMinReductionHashAggr(0.5f);

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    //Get the processing mode
    VectorGroupByOperator.ProcessingModeHashAggregate processingMode =
        (VectorGroupByOperator.ProcessingModeHashAggregate) vgo.processingMode;
    VectorAggregateExpression spyAggregator = spy(vgo.aggregators[0]);
    vgo.aggregators[0] = spyAggregator;

    FakeVectorRowBatchFromObjectIterables data = getDataForRollup();

    long countRowsProduced = 0;
    for (VectorizedRowBatch unit: data) {
      countRowsProduced += unit.size;
      vgo.process(unit,  0);

      // trigger flush frequently to simulate operator working on many batches
      processingMode.gcCanary.clear();

      if (countRowsProduced >= 1000) {
        break;
      }
    }

    vgo.close(false);
    // The exact number of allocations depend on input. In this case it is 13.
    // Without buffer reuse, we allocate 512 buffers for the same input
    verify(spyAggregator, times(13)).getNewAggregationBuffer();
  }

  @Test
  public void testRollupAggregationWithFlush() throws HiveException {

    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("k1");
    mapColumnNames.add("k2");
    mapColumnNames.add("v");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    // select count(v) from name group by rollup (k1,k2);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildKeyGroupByDesc (ctx, "count",
        "v", TypeInfoFactory.longTypeInfo,
        new String[] { "k1", "k2" },
        new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo});
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;

    desc.setGroupingSetsPresent(true);
    ArrayList<Long> groupingSets = new ArrayList<>();
    // groupingSets
    groupingSets.add(0L);
    groupingSets.add(1L);
    groupingSets.add(2L);
    desc.setListGroupingSets(groupingSets);
    // add grouping sets dummy key
    ExprNodeDesc groupingSetDummyKey = new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 0L);
    // this only works because we used an arraylist in buildKeyGroupByDesc
    // don't do this in actual compiler
    desc.getKeys().add(groupingSetDummyKey);
    // groupingSet Position
    desc.setGroupingSetPosition(2);

    CompilationOpContext cCtx = new CompilationOpContext();

    desc.setMinReductionHashAggr(0.5f);
    // Set really low check interval setting
    hconf.set("hive.groupby.mapaggr.checkinterval", "10");
    hconf.set("hive.vectorized.groupby.checkinterval", "10");

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    //Get the processing mode
    VectorGroupByOperator.ProcessingModeHashAggregate processingMode =
        (VectorGroupByOperator.ProcessingModeHashAggregate) vgo.processingMode;
    // No changes to the size of the hashtable due to grouping sets.
    assertEquals(1000000,
        ((VectorGroupByOperator.ProcessingModeHashAggregate)vgo.processingMode).getMaxHtEntries());

    this.outputRowCount = 0;
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {
      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        ++outputRowCount;
      }
    });

    FakeVectorRowBatchFromObjectIterables data = getDataForRollup();

    long countRowsProduced = 0;
    long numElementsToBeRetained = 0;
    int avgAccess = 0;
    for (VectorizedRowBatch unit: data) {
      countRowsProduced += unit.size;
      vgo.process(unit,  0);

      if (countRowsProduced >= 100) {
        // note down avg access
        avgAccess = processingMode.computeAvgAccess();
        numElementsToBeRetained = getElementsHigherThan(processingMode.mapKeysAggregationBuffers, avgAccess);
        // trigger flush explicitly on next iteration
        processingMode.gcCanary.clear();
        break;
      }
    }

    // This processing would trigger flush
    for (VectorizedRowBatch unit: data) {
      long zeroAccessBeforeFlush = getElementsWithZeroAccess(processingMode.mapKeysAggregationBuffers);
      vgo.process(unit,  0);
      long freqElementsAfterFlush = getElementsHigherThan(processingMode.mapKeysAggregationBuffers, avgAccess);

      assertTrue("After flush: " + freqElementsAfterFlush + ", before flush: " + numElementsToBeRetained,
          (freqElementsAfterFlush >= numElementsToBeRetained));

      // ensure that freq elements are reset for providing chance for others
      long zeroAccessAfterFlush = getElementsWithZeroAccess(processingMode.mapKeysAggregationBuffers);
      assertTrue("After flush: " + zeroAccessAfterFlush + ", before flush: " + zeroAccessBeforeFlush,
          (zeroAccessAfterFlush > zeroAccessBeforeFlush));

      break;
    }
    vgo.close(false);
  }

  long getElementsHigherThan(Map<KeyWrapper, VectorAggregationBufferRow> aggMap, int avgAccess) {
    return aggMap.values().stream().filter(v -> (v.getAccessCount() > avgAccess)).count();
  }

  long getElementsWithZeroAccess(Map<KeyWrapper, VectorAggregationBufferRow> aggMap) {
    return aggMap.values().stream().filter(v -> (v.getAccessCount() == 0)).count();
  }

  @Test
  public void testMaxHTEntriesFlush() throws HiveException {

    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("Key");
    mapColumnNames.add("Value");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildKeyGroupByDesc (ctx, "max",
        "Value", TypeInfoFactory.longTypeInfo,
        new String[] {"Key"},
        new TypeInfo[] {TypeInfoFactory.longTypeInfo});
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;

    // Set the memory threshold so that we get 100Kb before we need to flush.
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    long maxMemory = memoryMXBean.getHeapMemoryUsage().getMax();

    // 1 MB should be able to store 1M/16bytes(key+data) = 62500 entries
    float threshold = 10 * 100.0f*1024.0f/maxMemory;
    desc.setGroupByMemoryUsage(threshold);

    // Set really low MAXENTRIES setting
    hconf.set("hive.vectorized.groupby.maxentries", "100");

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    long expected = vgo.getMaxMemory();
    assertEquals(expected, maxMemory);

    this.outputRowCount = 0;
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {
      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        ++outputRowCount;
      }
    });

    Iterable<Object> it = new Iterable<Object>() {
      @Override
      public Iterator<Object> iterator() {
        return new Iterator<Object> () {
          long value = 0;

          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public Object next() {
            return ++value;
          }

          @Override
          public void remove() {
          }
        };
      }
    };

    FakeVectorRowBatchFromObjectIterables data = new FakeVectorRowBatchFromObjectIterables(
        100,
        new String[] {"long", "long"},
        it,
        it);

    // The 'it' data source will produce data w/o ever ending
    // We want to see that VGBY entries are NOT flushed when reaching 100
    // (misconfigured threshold) but when we reach about 30% of maxMemory
    long countRowsProduced = 0;
    for (VectorizedRowBatch unit: data) {
      countRowsProduced += 100;
      vgo.process(unit,  0);
      if (0 < outputRowCount) {
        break;
      }

    }
    // Make sure that we did not flush at the low entry threshold
    assertTrue( countRowsProduced > 100);
    // Make sure we did not go above 30% of available memory
    assertTrue(countRowsProduced < 0.3 * (1000 * 1024 / 16));
  }

  @Test
  public void testMultiKeyIntStringInt() throws HiveException {
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"int", "string", "int", "double"},
            Arrays.asList(new Object[]{null,   1,   1,  null,    2,    2, null}),
            Arrays.asList(new Object[]{ "A", "A",  "A", "C", null, null,  "A"}),
            Arrays.asList(new Object[]{null,   2,   2,  null,    2,    2, null}),
            Arrays.asList(new Object[]{1.0,  2.0, 4.0,   8.0, 16.0, 32.0, 64.0})),
        buildHashMap(
            Arrays.asList(   1,  "A",    2), 6.0,
            Arrays.asList(null,  "C", null), 8.0,
            Arrays.asList(   2, null,    2), 48.0,
            Arrays.asList(null,  "A", null), 65.0));
  }

  @Test
  public void testMultiKeyStringByteString() throws HiveException {
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            1,
            new String[] {"string", "tinyint", "string", "double"},
            Arrays.asList(new Object[]{"A", "A", null}),
            Arrays.asList(new Object[]{  1,  1,     1}),
            Arrays.asList(new Object[]{ "A", "A", "A"}),
            Arrays.asList(new Object[]{ 1.0, 1.0, 1.0})),
        buildHashMap(
            Arrays.asList( "A",    (byte)1,  "A"), 2.0,
            Arrays.asList( null,   (byte)1,  "A"), 1.0));
  }

  @Test
  public void testMultiKeyStringIntString() throws HiveException {
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"string", "int", "string", "double"},
            Arrays.asList(new Object[]{ "A", "A", "A", "C", null, null,  "A"}),
            Arrays.asList(new Object[]{null,   1,  1, null,    2,    2, null}),
            Arrays.asList(new Object[]{ "A", "A", "A", "C", null, null,  "A"}),
            Arrays.asList(new Object[]{ 1.0, 1.0,  1.0, 1.0, 1.0,  1.0,  1.0})),
        buildHashMap(
            Arrays.asList(null,    2, null), 2.0,
            Arrays.asList( "C", null,  "C"), 1.0,
            Arrays.asList( "A",    1,  "A"), 2.0,
            Arrays.asList( "A", null,  "A"), 2.0));
  }

  @Test
  public void testMultiKeyIntStringString() throws HiveException {
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"int", "string", "string", "double"},
            Arrays.asList(new Object[]{null,   1,  1, null,    2,    2, null}),
            Arrays.asList(new Object[]{ "A", "A", "A", "C", null, null,  "A"}),
            Arrays.asList(new Object[]{ "A", "A", "A", "C", null, null,  "A"}),
            Arrays.asList(new Object[]{ 1.0, 1.0,  1.0, 1.0, 1.0,  1.0,  1.0})),
        buildHashMap(
            Arrays.asList(   2, null, null), 2.0,
            Arrays.asList(null,  "C",  "C"), 1.0,
            Arrays.asList(   1,  "A",  "A"), 2.0,
            Arrays.asList(null,  "A",  "A"), 2.0));
  }

  @Test
  public void testMultiKeyDoubleStringInt() throws HiveException {
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"double", "string", "int", "double"},
            Arrays.asList(new Object[]{null,  1.0, 1.0,  null,  2.0,  2.0, null}),
            Arrays.asList(new Object[]{ "A", "A",  "A", "C",   null, null,  "A"}),
            Arrays.asList(new Object[]{null,   2,   2,  null,     2,    2, null}),
            Arrays.asList(new Object[]{1.0,  2.0, 4.0,   8.0, 16.0, 32.0, 64.0})),
        buildHashMap(
            Arrays.asList( 1.0,  "A",    2), 6.0,
            Arrays.asList(null,  "C", null), 8.0,
            Arrays.asList( 2.0, null,    2), 48.0,
            Arrays.asList(null,  "A", null), 65.0));
  }

  @Test
  public void testMultiKeyDoubleShortString() throws HiveException {
    short s = 2;
    testMultiKey(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"double", "smallint", "string", "double"},
            Arrays.asList(new Object[]{null,  1.0, 1.0,  null,  2.0,  2.0, null}),
            Arrays.asList(new Object[]{null,  s,     s,  null,    s,    s, null}),
            Arrays.asList(new Object[]{ "A", "A",  "A",   "C", null, null,  "A"}),
            Arrays.asList(new Object[]{1.0,  2.0,  4.0,   8.0, 16.0, 32.0,  64.0})),
        buildHashMap(
            Arrays.asList( 1.0,    s,  "A"), 6.0,
            Arrays.asList(null,  null, "C"), 8.0,
            Arrays.asList( 2.0,    s, null), 48.0,
            Arrays.asList(null, null,  "A"), 65.0));
  }


  @Test
  public void testDoubleValueTypeSum() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 20.0, null, 19.0));
  }

  @Test
  public void testDoubleValueTypeSumOneKey() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 39.0));
  }

  @Test
  public void testDoubleValueTypeCount() throws HiveException {
    testKeyTypeAggregate(
        "count",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 2L, null, 1L));
  }

  @Test
  public void testDoubleValueTypeCountOneKey() throws HiveException {
    testKeyTypeAggregate(
        "count",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 3L));
  }

  @Test
  public void testDoubleValueTypeAvg() throws HiveException {
    testKeyTypeAggregate(
        "avg",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 10.0, null, 19.0));
  }

  @Test
  public void testDoubleValueTypeAvgOneKey() throws HiveException {
    testKeyTypeAggregate(
        "avg",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 13.0));
  }

  @Test
  public void testDoubleValueTypeMin() throws HiveException {
    testKeyTypeAggregate(
        "min",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 7.0, null, 19.0));
  }

  @Test
  public void testDoubleValueTypeMinOneKey() throws HiveException {
    testKeyTypeAggregate(
        "min",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 7.0));
  }

  @Test
  public void testDoubleValueTypeMax() throws HiveException {
    testKeyTypeAggregate(
        "max",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 13.0, null, 19.0));
  }

  @Test
  public void testDoubleValueTypeMaxOneKey() throws HiveException {
    testKeyTypeAggregate(
        "max",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 19.0));
  }

  @Test
  public void testDoubleValueTypeVariance() throws HiveException {
    testKeyTypeAggregate(
        "variance",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 9.0, null, 0.0));
  }

  @Test
  public void testDoubleValueTypeVarianceOneKey() throws HiveException {
    testKeyTypeAggregate(
        "variance",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "double"},
            Arrays.asList(new Object[]{  1, 1, 1, 1}),
            Arrays.asList(new Object[]{13.0,null,7.0, 19.0})),
        buildHashMap((byte)1, 24.0));
  }
  @Test
  public void testTinyintKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"tinyint", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap((byte)1, 20L, null, 19L));
  }

  @Test
  public void testSmallintKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"smallint", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap((short)1, 20L, null, 19L));
  }

  @Test
  public void testIntKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"int", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(1, 20L, null, 19L));
  }

  @Test
  public void testBigintKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"bigint", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(1L, 20L, null, 19L));
  }

  @Test
  public void testBooleanKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"boolean", "bigint"},
            Arrays.asList(new Object[]{  true,null, true, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(true, 20L, null, 19L));
  }

  @Test
  public void testTimestampKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"timestamp", "bigint"},
            Arrays.asList(new Object[]{new Timestamp(1),null, new Timestamp(1), null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(new Timestamp(1), 20L, null, 19L));
  }

  @Test
  public void testFloatKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"float", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap((float)1.0, 20L, null, 19L));
  }

  @Test
  public void testDoubleKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"double", "bigint"},
            Arrays.asList(new Object[]{  1,null, 1, null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(1.0, 20L, null, 19L));
  }

  @Test
  public void testCountStar() throws HiveException {
    testAggregateCountStar(
        2,
        Arrays.asList(new Long[]{13L,null,7L,19L}),
        4L);
  }

  @Test
  public void testCountReduce() throws HiveException {
    testAggregateCountReduce(
            2,
            Arrays.asList(new Long[]{}),
            0L);
    testAggregateCountReduce(
            2,
            Arrays.asList(new Long[]{0L}),
            0L);
    testAggregateCountReduce(
            2,
            Arrays.asList(new Long[]{0L,0L}),
            0L);
    testAggregateCountReduce(
            2,
            Arrays.asList(new Long[]{0L,1L,0L}),
            1L);
    testAggregateCountReduce(
        2,
        Arrays.asList(new Long[]{13L,0L,7L,19L}),
        39L);
  }

  @Test
  public void testCountDecimal() throws HiveException {
    testAggregateDecimal(
        "Decimal",
        "count",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(1),
                HiveDecimal.create(2),
                HiveDecimal.create(3)}),
       3L);
  }

  @Test
  public void testMaxDecimal() throws HiveException {
    testAggregateDecimal(
        "Decimal",
        "max",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(1),
                HiveDecimal.create(2),
                HiveDecimal.create(3)}),
       HiveDecimal.create(3));
    testAggregateDecimal(
        "Decimal",
        "max",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(3),
                HiveDecimal.create(2),
                HiveDecimal.create(1)}),
        HiveDecimal.create(3));
    testAggregateDecimal(
        "Decimal",
        "max",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(2),
                HiveDecimal.create(3),
                HiveDecimal.create(1)}),
        HiveDecimal.create(3));
  }

  @Test
  public void testMinDecimal() throws HiveException {
    testAggregateDecimal(
        "Decimal",
        "min",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(1),
                HiveDecimal.create(2),
                HiveDecimal.create(3)}),
       HiveDecimal.create(1));
    testAggregateDecimal(
        "Decimal",
        "min",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(3),
                HiveDecimal.create(2),
                HiveDecimal.create(1)}),
        HiveDecimal.create(1));

    testAggregateDecimal(
        "Decimal",
        "min",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(2),
                HiveDecimal.create(1),
                HiveDecimal.create(3)}),
        HiveDecimal.create(1));
  }

  @Test
  public void testSumDecimal() throws HiveException {
    testAggregateDecimal(
        "Decimal",
       "sum",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(1),
                HiveDecimal.create(2),
                HiveDecimal.create(3)}),
       HiveDecimal.create(1+2+3));
  }

  @Test
  public void testSumDecimalHive6508() throws HiveException {
    short scale = 4;
    testAggregateDecimal(
        "Decimal(10,4)",
        "sum",
        4,
        Arrays.asList(new Object[]{
                HiveDecimal.create("1234.2401"),
                HiveDecimal.create("1868.52"),
                HiveDecimal.ZERO,
                HiveDecimal.create("456.84"),
                HiveDecimal.create("121.89")}),
       HiveDecimal.create("3681.4901"));
  }

  @Test
  public void testAvgDecimal() throws HiveException {
    testAggregateDecimal(
        "Decimal",
        "avg",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(1),
                HiveDecimal.create(2),
                HiveDecimal.create(3)}),
       HiveDecimal.create((1+2+3)/3));
  }

  @Test
  public void testAvgDecimalNegative() throws HiveException {
    testAggregateDecimal(
        "Decimal",
        "avg",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(-1),
                HiveDecimal.create(-2),
                HiveDecimal.create(-3)}),
        HiveDecimal.create((-1-2-3)/3));
  }

  @Test
  public void testVarianceDecimal () throws HiveException {
      testAggregateDecimal(
        "Decimal",
        "variance",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(13),
                HiveDecimal.create(5),
                HiveDecimal.create(7),
                HiveDecimal.create(19)}),
        (double) 30);
  }

  @Test
  public void testVarSampDecimal () throws HiveException {
      testAggregateDecimal(
        "Decimal",
        "var_samp",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(13),
                HiveDecimal.create(5),
                HiveDecimal.create(7),
                HiveDecimal.create(19)}),
        (double) 40);
  }

  @Test
  public void testStdPopDecimal () throws HiveException {
      testAggregateDecimal(
        "Decimal",
        "stddev_pop",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(13),
                HiveDecimal.create(5),
                HiveDecimal.create(7),
                HiveDecimal.create(19)}),
        Math.sqrt(30));
  }

  @Test
  public void testStdSampDecimal () throws HiveException {
      testAggregateDecimal(
        "Decimal",
        "stddev_samp",
        2,
        Arrays.asList(new Object[]{
                HiveDecimal.create(13),
                HiveDecimal.create(5),
                HiveDecimal.create(7),
                HiveDecimal.create(19)}),
        Math.sqrt(40));
  }

  @Test
  public void testDecimalKeyTypeAggregate() throws HiveException {
    testKeyTypeAggregate(
        "sum",
        new FakeVectorRowBatchFromObjectIterables(
            2,
            new String[] {"decimal(38,0)", "bigint"},
            Arrays.asList(new Object[]{
                    HiveDecimal.create(1),null,
                    HiveDecimal.create(1), null}),
            Arrays.asList(new Object[]{13L,null,7L, 19L})),
        buildHashMap(HiveDecimal.create(1), 20L, null, 19L));
  }


  @Test
  public void testCountString() throws HiveException {
    testAggregateString(
        "count",
        2,
        Arrays.asList(new Object[]{"A","B","C"}),
        3L);
  }

  @Test
  public void testMaxString() throws HiveException {
    testAggregateString(
        "max",
        2,
        Arrays.asList(new Object[]{"A","B","C"}),
        "C");
    testAggregateString(
        "max",
        2,
        Arrays.asList(new Object[]{"C", "B", "A"}),
        "C");
  }

  @Test
  public void testMinString() throws HiveException {
    testAggregateString(
        "min",
        2,
        Arrays.asList(new Object[]{"A","B","C"}),
        "A");
    testAggregateString(
        "min",
        2,
        Arrays.asList(new Object[]{"C", "B", "A"}),
        "A");
  }

  @Test
  public void testMaxNullString() throws HiveException {
    testAggregateString(
        "max",
        2,
        Arrays.asList(new Object[]{"A","B",null}),
        "B");
    testAggregateString(
        "max",
        2,
        Arrays.asList(new Object[]{null, null, null}),
        null);
  }

  @Test
  public void testCountStringWithNull() throws HiveException {
    testAggregateString(
        "count",
        2,
        Arrays.asList(new Object[]{"A",null,"C", "D", null}),
        3L);
  }

  @Test
  public void testCountStringAllNull() throws HiveException {
    testAggregateString(
        "count",
        4,
        Arrays.asList(new Object[]{null, null, null, null, null}),
        0L);
  }


  @Test
  public void testMinLongNullStringKeys() throws HiveException {
    testAggregateStringKeyAggregate(
        "min",
        2,
        Arrays.asList(new Object[]{"A",null,"A",null}),
        Arrays.asList(new Object[]{13L, 5L, 7L,19L}),
        buildHashMap("A", 7L, null, 5L));
  }

  @Test
  public void testMinLongStringKeys() throws HiveException {
    testAggregateStringKeyAggregate(
        "min",
        2,
        Arrays.asList(new Object[]{"A","B","A","B"}),
        Arrays.asList(new Object[]{13L, 5L, 7L,19L}),
        buildHashMap("A", 7L, "B", 5L));
  }

  @Test
  public void testMinLongKeyGroupByCompactBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{01L,1L,2L,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(1L, 5L, 2L, 7L));
  }

  @Test
  public void testMinLongKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        4,
        Arrays.asList(new Long[]{01L,1L,2L,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(1L, 5L, 2L, 7L));
  }

  @Test
  public void testMinLongKeyGroupByCrossBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{01L,2L,1L,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(1L, 7L, 2L, 5L));
  }

  @Test
  public void testMinLongNullKeyGroupByCrossBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 7L, 2L, 5L));
  }

  @Test
  public void testMinLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 7L, 2L, 5L));
  }

  @Test
  public void testMaxLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "max",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 13L, 2L, 19L));
  }

  @Test
  public void testCountLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "count",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 2L, 2L, 2L));
  }

  @Test
  public void testSumLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "sum",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 20L, 2L, 24L));
  }

  @Test
  public void testAvgLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "avg",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        buildHashMap(null, 10.0, 2L, 12.0));
  }

  @Test
  public void testVarLongNullKeyGroupBySingleBatch() throws HiveException {
    testAggregateLongKeyAggregate(
        "variance",
        4,
        Arrays.asList(new Long[]{null,2L,01L,02L,01L,01L}),
        Arrays.asList(new Long[]{13L, 5L,18L,19L,12L,15L}),
        buildHashMap(null, 0.0, 2L, 49.0, 01L, 6.0));
  }

  @Test
  public void testMinNullLongNullKeyGroupBy() throws HiveException {
    testAggregateLongKeyAggregate(
        "min",
        4,
        Arrays.asList(new Long[]{null,2L,null,02L}),
        Arrays.asList(new Long[]{null, null, null, null}),
        buildHashMap(null, null, 2L, null));
  }

  @Test
  public void testMinLongGroupBy() throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        5L);
  }


  @Test
  public void testMinLongSimple() throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        5L);
  }

  @Test
  public void testMinLongEmpty() throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{}),
        null);
  }

  @Test
  public void testMinLongNulls() throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{null}),
        null);
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{null, null, null}),
        null);
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{null,5L,7L,19L}),
        5L);
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,null,7L,19L}),
        7L);
  }

  @Test
  public void testMinLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "min",
        42L,
        4096,
        1024,
        42L);
  }

  @Test
  public void testMinLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "min",
        null,
        4096,
        1024,
        null);
  }


  @Test
  public void testMinLongNegative () throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,-19L}),
        -19L);
  }

  @Test
  public void testMinLongMinInt () throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,5L,(long)Integer.MIN_VALUE,-19L}),
        (long)Integer.MIN_VALUE);
  }

  @Test
  public void testMinLongMinLong () throws HiveException {
    testAggregateLongAggregate(
        "min",
        2,
        Arrays.asList(new Long[]{13L,5L, Long.MIN_VALUE, (long)Integer.MIN_VALUE}),
        Long.MIN_VALUE);
  }

  @Test
  public void testMaxLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "max",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        19L);
  }

  @Test
  public void testMaxLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "max",
        2,
        Arrays.asList(new Long[]{}),
        null);
  }


  @Test
  public void testMaxLongNegative () throws HiveException {
    testAggregateLongAggregate(
        "max",
        2,
        Arrays.asList(new Long[]{-13L,-5L,-7L,-19L}),
        -5L);
  }

  @Test
  public void testMaxLongMaxInt () throws HiveException {
    testAggregateLongAggregate(
        "max",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,(long)Integer.MAX_VALUE}),
        (long)Integer.MAX_VALUE);
  }

  @Test
  public void testMaxLongMaxLong () throws HiveException {
    testAggregateLongAggregate(
        "max",
        2,
        Arrays.asList(new Long[]{13L,Long.MAX_VALUE - 1L,Long.MAX_VALUE,(long)Integer.MAX_VALUE}),
        Long.MAX_VALUE);
  }

  @Test
  public void testMaxLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "max",
        42L,
        4096,
        1024,
        42L);
  }

  @Test
  public void testMaxLongNulls () throws HiveException {
    testAggregateLongRepeats (
        "max",
        null,
        4096,
        1024,
        null);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMinLongConcatRepeat () throws HiveException {
    testAggregateLongIterable ("min",
        new FakeVectorRowBatchFromConcat(
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2),
            new FakeVectorRowBatchFromRepeats(
                new Long[] {7L}, 15, 2),
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2)),
         7L);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMinLongRepeatConcatValues () throws HiveException {
    testAggregateLongIterable ("min",
        new FakeVectorRowBatchFromConcat(
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2),
            new FakeVectorRowBatchFromLongIterables(
                3,
                Arrays.asList(new Long[]{13L, 7L, 23L, 29L}))),
         7L);
  }

  @Test
  public void testCountLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        4L);
  }

  @Test
  public void testCountLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{}),
        0L);
  }

  @Test
  public void testCountLongNulls () throws HiveException {
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{null}),
        0L);
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{null, null, null}),
        0L);
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{null,5L,7L,19L}),
        3L);
    testAggregateLongAggregate(
        "count",
        2,
        Arrays.asList(new Long[]{13L,null,7L,19L}),
        3L);
  }


  @Test
  public void testCountLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "count",
        42L,
        4096,
        1024,
        4096L);
  }

  @Test
  public void testCountLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "count",
        null,
        4096,
        1024,
        0L);
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testCountLongRepeatConcatValues () throws HiveException {
    testAggregateLongIterable ("count",
        new FakeVectorRowBatchFromConcat(
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2),
            new FakeVectorRowBatchFromLongIterables(
                3,
                Arrays.asList(new Long[]{13L, 7L, 23L, 29L}))),
         14L);
  }

  @Test
  public void testSumDoubleSimple() throws HiveException {
    testAggregateDouble(
        "sum",
        2,
        Arrays.asList(new Object[]{13.0,5.0,7.0,19.0}),
        13.0 + 5.0 + 7.0 + 19.0);
  }

  @Test
  public void testSumDoubleGroupByString() throws HiveException {
    testAggregateDoubleStringKeyAggregate(
        "sum",
        4,
        Arrays.asList(new Object[]{"A", null, "A", null}),
        Arrays.asList(new Object[]{13.0,5.0,7.0,19.0}),
        buildHashMap("A", 20.0, null, 24.0));
  }

  @Test
  public void testSumLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        13L + 5L + 7L + 19L);
  }

  @Test
  public void testSumLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{}),
        null);
  }

  @Test
  public void testSumLongNulls () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{null}),
        null);
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{null, null, null}),
        null);
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{null,5L,7L,19L}),
        5L + 7L + 19L);
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{13L,null,7L,19L}),
        13L + 7L + 19L);
  }

  @Test
  public void testSumLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "sum",
        42L,
        4096,
        1024,
        4096L * 42L);
  }

  @Test
  public void testSumLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "sum",
        null,
        4096,
        1024,
        null);
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testSumLongRepeatConcatValues () throws HiveException {
    testAggregateLongIterable ("sum",
        new FakeVectorRowBatchFromConcat(
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2),
            new FakeVectorRowBatchFromLongIterables(
                3,
                Arrays.asList(new Long[]{13L, 7L, 23L, 29L}))),
         19L*10L + 13L + 7L + 23L +29L);
  }

  @Test
  public void testSumLongZero () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{-(long)Integer.MAX_VALUE, (long)Integer.MAX_VALUE}),
        0L);
  }

  @Test
  public void testSumLong2MaxInt () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{(long)Integer.MAX_VALUE, (long)Integer.MAX_VALUE}),
        4294967294L);
  }

  @Test
  public void testSumLong2MinInt () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{(long)Integer.MIN_VALUE, (long)Integer.MIN_VALUE}),
        -4294967296L);
  }

  @Test
  public void testSumLong2MaxLong () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{Long.MAX_VALUE, Long.MAX_VALUE}),
        -2L); // silent overflow
  }

  @Test
  public void testSumLong2MinLong () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{Long.MIN_VALUE, Long.MIN_VALUE}),
        0L); // silent overflow
  }

  @Test
  public void testSumLongMinMaxLong () throws HiveException {
    testAggregateLongAggregate(
        "sum",
        2,
        Arrays.asList(new Long[]{Long.MAX_VALUE, Long.MIN_VALUE}),
        -1L);
  }

  @Test
  public void testAvgLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        (double) (13L + 5L + 7L + 19L) / (double) 4L);
  }

  @Test
  public void testAvgLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{}),
        0.0);
  }

  @Test
  public void testAvgLongNulls () throws HiveException {
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{null}),
        0.0);
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{null, null, null}),
        0.0);
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{null,5L,7L,19L}),
        (double) (5L + 7L + 19L) / (double) 3L);
    testAggregateLongAggregate(
        "avg",
        2,
        Arrays.asList(new Long[]{13L,null,7L,19L}),
        (double) (13L + + 7L + 19L) / (double) 3L);
  }


  @Test
  public void testAvgLongRepeat () throws HiveException
  {
    testAggregateLongRepeats (
        "avg",
        42L,
        4096,
        1024,
        (double)42);
  }

  @Test
  public void testAvgLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "avg",
        null,
        4096,
        1024,
        0.0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAvgLongRepeatConcatValues () throws HiveException {
    testAggregateLongIterable ("avg",
        new FakeVectorRowBatchFromConcat(
            new FakeVectorRowBatchFromRepeats(
                new Long[] {19L}, 10, 2),
            new FakeVectorRowBatchFromLongIterables(
                3,
                Arrays.asList(new Long[]{13L, 7L, 23L, 29L}))),
         (double) (19L*10L + 13L + 7L + 23L +29L) / (double) 14 );
  }

  @Test
  public void testVarianceLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        (double) 30L);
  }

  @Test
  public void testVarianceLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{}),
        0.0);
  }

  @Test
  public void testVarianceLongSingle () throws HiveException {
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{97L}),
        0.0);
  }

  @Test
  public void testVarianceLongNulls () throws HiveException {
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{null}),
        0.0);
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{null, null, null}),
        0.0);
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{null,13L, 5L,7L,19L}),
        30.0);
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{13L,null,5L, 7L,19L}),
        30.0);
    testAggregateLongAggregate(
        "variance",
        2,
        Arrays.asList(new Long[]{null,null,null,19L}),
        (double) 0);
  }

  @Test
  public void testVarPopLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "var_pop",
        null,
        4096,
        1024,
        0.0);
  }

  @Test
  public void testVarPopLongRepeat () throws HiveException  {
    testAggregateLongRepeats (
        "var_pop",
        42L,
        4096,
        1024,
        (double)0);
  }

  @Test
  public void testVarSampLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "var_samp",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        (double) 40L);
  }

  @Test
  public void testVarSampLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "var_samp",
        2,
        Arrays.asList(new Long[]{}),
        0.0);
  }


  @Test
  public void testVarSampLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "var_samp",
        42L,
        4096,
        1024,
        (double)0);
  }

  @Test
  public void testStdLongSimple () throws HiveException {
    testAggregateLongAggregate(
        "std",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        Math.sqrt(30));
  }

  @Test
  public void testStdLongEmpty () throws HiveException {
    testAggregateLongAggregate(
        "std",
        2,
        Arrays.asList(new Long[]{}),
        0.0);
  }


  @Test
  public void testStdDevLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "stddev",
        42L,
        4096,
        1024,
        (double)0);
  }

  @Test
  public void testStdDevLongRepeatNulls () throws HiveException {
    testAggregateLongRepeats (
        "stddev",
        null,
        4096,
        1024,
        0.0);
  }


  @Test
  public void testStdDevSampSimple () throws HiveException {
    testAggregateLongAggregate(
        "stddev_samp",
        2,
        Arrays.asList(new Long[]{13L,5L,7L,19L}),
        Math.sqrt(40));
  }

  @Test
  public void testStdDevSampLongRepeat () throws HiveException {
    testAggregateLongRepeats (
        "stddev_samp",
        42L,
        3,
        1024,
        (double)0);
  }

  @Test
  public void testInstantiateExpressionMonoParameterExpression() throws Exception {
    VectorGroupByOperator op = new VectorGroupByOperator();
    VectorAggregationDesc desc = Mockito.mock(VectorAggregationDesc.class);
    Mockito.when(desc.getVecAggrClass()).thenReturn((Class) VectorUDAFCount.class);
    VectorAggregateExpression expr = op.instantiateExpression(desc);
    Assert.assertEquals(VectorUDAFCount.class, expr.getClass());
  }

  @Test
  public void testInstantiateExpressionMonoParameterWhenAlternatives() throws Exception {
    VectorGroupByOperator op = new VectorGroupByOperator();
    // VectorUDAFBloomFilterMerge with "regular" constructor (single parameter)
    VectorAggregationDesc desc = Mockito.mock(VectorAggregationDesc.class);
    Mockito.when(desc.getVecAggrClass()).thenReturn((Class) VectorUDAFBloomFilterMerge.class);
    Mockito.when(desc.getEvaluator())
        .thenReturn(new GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator());
    VectorAggregateExpression expr = op.instantiateExpression(desc);
    Assert.assertEquals(VectorUDAFBloomFilterMerge.class, expr.getClass());
  }

  @Test
  public void testInstantiateExpressionMultiParameterExpression() throws Exception {
    VectorGroupByOperator op = new VectorGroupByOperator();
    // VectorUDAFBloomFilterMerge with specific constructor (additional constant parameter)
    VectorAggregationDesc desc = Mockito.mock(VectorAggregationDesc.class);
    Mockito.when(desc.getVecAggrClass()).thenReturn((Class) VectorUDAFBloomFilterMerge.class);
    Mockito.when(desc.getEvaluator())
        .thenReturn(new GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator());
    TypeInfo intTypeInfo = TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(Integer.TYPE);
    Mockito.when(desc.getConstants()).thenReturn(
        Collections.singletonList(ConstantVectorExpression.create(2, 16, intTypeInfo)));
    VectorAggregateExpression expr = op.instantiateExpression(desc);
    Assert.assertEquals(VectorUDAFBloomFilterMerge.class, expr.getClass());
  }

  @Test
  public void testInstantiateExpressionNonPrimitiveConstantType_KO() throws Exception {
    VectorGroupByOperator op = new VectorGroupByOperator();
    // VectorUDAFBloomFilterMerge with specific constructor (additional constant parameter)
    VectorAggregationDesc desc = Mockito.mock(VectorAggregationDesc.class);
    Mockito.when(desc.getVecAggrClass()).thenReturn((Class) VectorUDAFBloomFilterMerge.class);
    Mockito.when(desc.getEvaluator())
        .thenReturn(new GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator());
    TypeInfo intTypeInfo = TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(Integer.TYPE);
    TypeInfo structTypeInfo = TypeInfoFactory.getStructTypeInfo(
        Collections.singletonList("first"), Collections.singletonList(intTypeInfo));
    Mockito.when(desc.getConstants()).thenReturn(
        Collections.singletonList(ConstantVectorExpression.create(2, Arrays.asList(16), structTypeInfo)));
    assertThrows(IllegalArgumentException.class, () -> op.instantiateExpression(desc));
  }

  private void testMultiKey(
      String aggregateName,
      FakeVectorRowBatchFromObjectIterables data,
      HashMap<Object, Object> expected) throws HiveException {

    Map<String, Integer> mapColumnNames = new HashMap<String, Integer>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    ArrayList<ExprNodeDesc> keysDesc = new ArrayList<ExprNodeDesc>();
    Set<Object> keys = new HashSet<Object>();

    // The types array tells us the number of columns in the data
    final String[] columnTypes = data.getTypes();

    // Columns 0..N-1 are keys. Column N is the aggregate value input
    int i=0;
    for(; i<columnTypes.length - 1; ++i) {
      String columnName = String.format("_col%d", i);
      mapColumnNames.put(columnName, i);
      outputColumnNames.add(columnName);
    }

    mapColumnNames.put("value", i);
    outputColumnNames.add("value");
    VectorizationContext ctx = new VectorizationContext("name", outputColumnNames);

    ArrayList<AggregationDesc> aggs = new ArrayList(1);
    aggs.add(
        buildAggregationDesc(ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1,
            "value", TypeInfoFactory.getPrimitiveTypeInfo(columnTypes[i])));

    for(i=0; i<columnTypes.length - 1; ++i) {
      String columnName = String.format("_col%d", i);
      keysDesc.add(
        buildColumnDesc(ctx, columnName,
            TypeInfoFactory.getPrimitiveTypeInfo(columnTypes[i])));
    }

    GroupByDesc desc = new GroupByDesc();
    VectorGroupByDesc vectorGroupByDesc = new VectorGroupByDesc();

    desc.setOutputColumnNames(outputColumnNames);
    desc.setAggregators(aggs);
    desc.setKeys(keysDesc);
    vectorGroupByDesc.setProcessingMode(ProcessingMode.HASH);

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorGroupByDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {

      private int rowIndex;
      private String aggregateName;
      private Map<Object,Object> expected;
      private Set<Object> keys;

      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        assertTrue(row instanceof Object[]);
        Object[] fields = (Object[]) row;
        assertEquals(columnTypes.length, fields.length);
        ArrayList<Object> keyValue = new ArrayList<Object>(columnTypes.length-1);
        for(int i=0; i<columnTypes.length-1; ++i) {
          Object key = fields[i];
          if (null == key) {
            keyValue.add(null);
          } else if (key instanceof Text) {
            Text txKey = (Text)key;
            keyValue.add(txKey.toString());
          } else if (key instanceof ByteWritable) {
            ByteWritable bwKey = (ByteWritable)key;
            keyValue.add(bwKey.get());
          } else if (key instanceof ShortWritable) {
            ShortWritable swKey = (ShortWritable)key;
            keyValue.add(swKey.get());
          } else if (key instanceof IntWritable) {
            IntWritable iwKey = (IntWritable)key;
            keyValue.add(iwKey.get());
          } else if (key instanceof LongWritable) {
            LongWritable lwKey = (LongWritable)key;
            keyValue.add(lwKey.get());
          } else if (key instanceof TimestampWritableV2) {
            TimestampWritableV2 twKey = (TimestampWritableV2)key;
            keyValue.add(twKey.getTimestamp());
          } else if (key instanceof DoubleWritable) {
            DoubleWritable dwKey = (DoubleWritable)key;
            keyValue.add(dwKey.get());
          } else if (key instanceof FloatWritable) {
            FloatWritable fwKey = (FloatWritable)key;
            keyValue.add(fwKey.get());
          } else if (key instanceof BooleanWritable) {
            BooleanWritable bwKey = (BooleanWritable)key;
            keyValue.add(bwKey.get());
          } else {
            Assert.fail(String.format("Not implemented key output type %s: %s",
                key.getClass().getName(), key));
          }
        }

        String keyAsString = Arrays.deepToString(keyValue.toArray());
        assertTrue(expected.containsKey(keyValue));
        Object expectedValue = expected.get(keyValue);
        Object value = fields[columnTypes.length-1];
        Validator validator = getValidator(aggregateName);
        validator.validate(keyAsString, expectedValue, new Object[] {value});
        keys.add(keyValue);
      }

      private FakeCaptureVectorToRowOutputOperator.OutputInspector init(
          String aggregateName, Map<Object,Object> expected, Set<Object> keys) {
        this.aggregateName = aggregateName;
        this.expected = expected;
        this.keys = keys;
        return this;
      }
    }.init(aggregateName, expected, keys));

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(expected.size(), outBatchList.size());
    assertEquals(expected.size(), keys.size());
  }


  private void testKeyTypeAggregate(
      String aggregateName,
      FakeVectorRowBatchFromObjectIterables data,
      Map<Object, Object> expected) throws HiveException {

    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("Key");
    mapColumnNames.add("Value");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);
    Set<Object> keys = new HashSet<Object>();

    AggregationDesc agg = buildAggregationDesc(ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1,
        "Value", TypeInfoFactory.getPrimitiveTypeInfo(data.getTypes()[1]));
    ArrayList<AggregationDesc> aggs = new ArrayList<AggregationDesc>();
    aggs.add(agg);

    ArrayList<String> outputColumnNames = new ArrayList<String>();
    outputColumnNames.add("_col0");
    outputColumnNames.add("_col1");

    GroupByDesc desc = new GroupByDesc();
    VectorGroupByDesc vectorGroupByDesc = new VectorGroupByDesc();

    desc.setOutputColumnNames(outputColumnNames);
    desc.setAggregators(aggs);
    vectorGroupByDesc.setProcessingMode(ProcessingMode.HASH);

    ExprNodeDesc keyExp = buildColumnDesc(ctx, "Key",
        TypeInfoFactory.getPrimitiveTypeInfo(data.getTypes()[0]));
    ArrayList<ExprNodeDesc> keysDesc = new ArrayList<ExprNodeDesc>();
    keysDesc.add(keyExp);
    desc.setKeys(keysDesc);

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorGroupByDesc);
    if (vgo == null) {
      assertTrue(false);
    }

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {

      private int rowIndex;
      private String aggregateName;
      private Map<Object,Object> expected;
      private Set<Object> keys;

      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        assertTrue(row instanceof Object[]);
        Object[] fields = (Object[]) row;
        assertEquals(2, fields.length);
        Object key = fields[0];
        Object keyValue = null;
        if (null == key) {
          keyValue = null;
        } else if (key instanceof ByteWritable) {
          ByteWritable bwKey = (ByteWritable)key;
          keyValue = bwKey.get();
        } else if (key instanceof ShortWritable) {
          ShortWritable swKey = (ShortWritable)key;
          keyValue = swKey.get();
        } else if (key instanceof IntWritable) {
          IntWritable iwKey = (IntWritable)key;
          keyValue = iwKey.get();
        } else if (key instanceof LongWritable) {
          LongWritable lwKey = (LongWritable)key;
          keyValue = lwKey.get();
        } else if (key instanceof TimestampWritableV2) {
          TimestampWritableV2 twKey = (TimestampWritableV2)key;
          keyValue = twKey.getTimestamp().toSqlTimestamp();
        } else if (key instanceof DoubleWritable) {
          DoubleWritable dwKey = (DoubleWritable)key;
          keyValue = dwKey.get();
        } else if (key instanceof FloatWritable) {
          FloatWritable fwKey = (FloatWritable)key;
          keyValue = fwKey.get();
        } else if (key instanceof BooleanWritable) {
          BooleanWritable bwKey = (BooleanWritable)key;
          keyValue = bwKey.get();
        } else if (key instanceof HiveDecimalWritable) {
            HiveDecimalWritable hdwKey = (HiveDecimalWritable)key;
            keyValue = hdwKey.getHiveDecimal();
        } else {
          Assert.fail(String.format("Not implemented key output type %s: %s",
              key.getClass().getName(), key));
        }

        String keyValueAsString = String.format("%s", keyValue);

        assertTrue(expected.containsKey(keyValue));
        Object expectedValue = expected.get(keyValue);
        Object value = fields[1];
        Validator validator = getValidator(aggregateName);
        validator.validate(keyValueAsString, expectedValue, new Object[] {value});
        keys.add(keyValue);
      }

      private FakeCaptureVectorToRowOutputOperator.OutputInspector init(
          String aggregateName, Map<Object,Object> expected, Set<Object> keys) {
        this.aggregateName = aggregateName;
        this.expected = expected;
        this.keys = keys;
        return this;
      }
    }.init(aggregateName, expected, keys));

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(expected.size(), outBatchList.size());
    assertEquals(expected.size(), keys.size());
  }


  public void testAggregateLongRepeats (
    String aggregateName,
    Long value,
    int repeat,
    int batchSize,
    Object expected) throws HiveException {
    FakeVectorRowBatchFromRepeats fdr = new FakeVectorRowBatchFromRepeats(
        new Long[] {value}, repeat, batchSize);
    testAggregateLongIterable (aggregateName, fdr, expected);
  }

  public HashMap<Object, Object> buildHashMap(Object... pairs) {
    HashMap<Object, Object> map = new HashMap<Object, Object>();
    for(int i = 0; i < pairs.length; i += 2) {
      map.put(pairs[i], pairs[i+1]);
    }
    return map;
  }

  public void testAggregateStringKeyAggregate (
      String aggregateName,
      int batchSize,
      Iterable<Object> list,
      Iterable<Object> values,
      HashMap<Object, Object> expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromObjectIterables fdr = new FakeVectorRowBatchFromObjectIterables(
        batchSize,
        new String[] {"string", "long"},
        list,
        values);
    testAggregateStringKeyIterable (aggregateName, fdr,  TypeInfoFactory.longTypeInfo, expected);
  }

  public void testAggregateDoubleStringKeyAggregate (
      String aggregateName,
      int batchSize,
      Iterable<Object> list,
      Iterable<Object> values,
      HashMap<Object, Object> expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromObjectIterables fdr = new FakeVectorRowBatchFromObjectIterables(
        batchSize,
        new String[] {"string", "double"},
        list,
        values);
    testAggregateStringKeyIterable (aggregateName, fdr,  TypeInfoFactory.doubleTypeInfo, expected);
  }

  public void testAggregateLongKeyAggregate (
      String aggregateName,
      int batchSize,
      List<Long> list,
      Iterable<Long> values,
      HashMap<Object, Object> expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromLongIterables fdr = new FakeVectorRowBatchFromLongIterables(batchSize,
        list, values);
    testAggregateLongKeyIterable (aggregateName, fdr, expected);
  }

  public void testAggregateDecimal (
      String typeName,
      String aggregateName,
      int batchSize,
      Iterable<Object> values,
      Object expected) throws HiveException {

        @SuppressWarnings("unchecked")
        FakeVectorRowBatchFromObjectIterables fdr = new FakeVectorRowBatchFromObjectIterables(
            batchSize, new String[] {typeName}, values);
        testAggregateDecimalIterable (aggregateName, fdr, expected);
      }


  public void testAggregateString (
      String aggregateName,
      int batchSize,
      Iterable<Object> values,
      Object expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromObjectIterables fdr = new FakeVectorRowBatchFromObjectIterables(
        batchSize, new String[] {"string"}, values);
    testAggregateStringIterable (aggregateName, fdr, expected);
  }

  public void testAggregateDouble (
      String aggregateName,
      int batchSize,
      Iterable<Object> values,
      Object expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromObjectIterables fdr = new FakeVectorRowBatchFromObjectIterables(
        batchSize, new String[] {"double"}, values);
    testAggregateDoubleIterable (aggregateName, fdr, expected);
  }


  public void testAggregateLongAggregate (
      String aggregateName,
      int batchSize,
      Iterable<Long> values,
      Object expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromLongIterables fdr = new FakeVectorRowBatchFromLongIterables(batchSize,
        values);
    testAggregateLongIterable (aggregateName, fdr, expected);
  }

  public void testAggregateCountStar (
      int batchSize,
      Iterable<Long> values,
      Object expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromLongIterables fdr = new FakeVectorRowBatchFromLongIterables(batchSize,
        values);
    testAggregateCountStarIterable (fdr, expected);
  }

  public void testAggregateCountReduce (
      int batchSize,
      Iterable<Long> values,
      Object expected) throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromLongIterables fdr = new FakeVectorRowBatchFromLongIterables(batchSize,
        values);
    testAggregateCountReduceIterable (fdr, expected);
  }


  public static interface Validator {
    void validate (String key, Object expected, Object result);
  };

  public static class ValueValidator implements Validator {
    @Override
    public void validate(String key, Object expected, Object result) {

      assertEquals(true, result instanceof Object[]);
      Object[] arr = (Object[]) result;
      assertEquals(1, arr.length);

      if (expected == null) {
        assertEquals(key, null, arr[0]);
      } else if (arr[0] instanceof LongWritable) {
        LongWritable lw = (LongWritable) arr[0];
        assertEquals(key, expected, lw.get());
      } else if (arr[0] instanceof Text) {
        Text tx = (Text) arr[0];
        String sbw = tx.toString();
        assertEquals(key, expected, sbw);
      } else if (arr[0] instanceof DoubleWritable) {
        DoubleWritable dw = (DoubleWritable) arr[0];
        assertEquals (key, expected, dw.get());
      } else if (arr[0] instanceof Double) {
        assertEquals (key, expected, arr[0]);
      } else if (arr[0] instanceof Long) {
        assertEquals (key, expected, arr[0]);
      } else if (arr[0] instanceof HiveDecimalWritable) {
        HiveDecimalWritable hdw = (HiveDecimalWritable) arr[0];
        HiveDecimal hd = hdw.getHiveDecimal();
        HiveDecimal expectedDec = (HiveDecimal)expected;
        assertEquals (key, expectedDec, hd);
      } else if (arr[0] instanceof HiveDecimal) {
          HiveDecimal hd = (HiveDecimal) arr[0];
          HiveDecimal expectedDec = (HiveDecimal)expected;
          assertEquals (key, expectedDec, hd);
      } else {
        Assert.fail("Unsupported result type: " + arr[0].getClass().getName());
      }
    }
  }

  public static class AvgValidator implements Validator {

    @Override
    public void validate(String key, Object expected, Object result) {
      Object[] arr = (Object[]) result;
      assertEquals (1, arr.length);

      if (expected == null) {
        assertEquals(key, null, arr[0]);
      } else {
        assertEquals (true, arr[0] instanceof Object[]);
        Object[] vals = (Object[]) arr[0];
        assertEquals (3, vals.length);

        assertEquals (true, vals[0] instanceof LongWritable);
        LongWritable lw = (LongWritable) vals[0];

        if (vals[1] instanceof DoubleWritable) {
          DoubleWritable dw = (DoubleWritable) vals[1];
          if (lw.get() != 0L) {
            assertEquals (key, expected, dw.get() / lw.get());
          } else {
            assertEquals(key, expected, 0.0);
          }
        } else if (vals[1] instanceof HiveDecimalWritable) {
          HiveDecimalWritable hdw = (HiveDecimalWritable) vals[1];
          if (lw.get() != 0L) {
            assertEquals (key, expected, hdw.getHiveDecimal().divide(HiveDecimal.create(lw.get())));
          } else {
            assertEquals(key, expected, HiveDecimal.ZERO);
          }
        }
      }
    }

  }

  public abstract static class BaseVarianceValidator implements Validator {

    abstract void validateVariance (String key,
        double expected, long cnt, double sum, double variance);

    @Override
    public void validate(String key, Object expected, Object result) {
      Object[] arr = (Object[]) result;
      assertEquals (1, arr.length);

      if (expected == null) {
        assertEquals(null, arr[0]);
      } else {
        assertEquals (true, arr[0] instanceof Object[]);
        Object[] vals = (Object[]) arr[0];
        assertEquals (3, vals.length);

        assertEquals (true, vals[0] instanceof LongWritable);
        assertEquals (true, vals[1] instanceof DoubleWritable);
        assertEquals (true, vals[2] instanceof DoubleWritable);
        LongWritable cnt = (LongWritable) vals[0];
        if (cnt.get() == 0) {
          assertEquals(key, expected, 0.0);
        } else {
          DoubleWritable sum = (DoubleWritable) vals[1];
          DoubleWritable var = (DoubleWritable) vals[2];
          assertTrue (1 <= cnt.get());
          validateVariance (key, (Double) expected, cnt.get(), sum.get(), var.get());
        }
      }
    }
  }

  public static class VarianceValidator extends BaseVarianceValidator {

    @Override
    void validateVariance(String key, double expected, long cnt, double sum, double variance) {
      assertEquals (key, expected, variance /cnt, 0.0);
    }
  }

  public static class VarianceSampValidator extends BaseVarianceValidator {

    @Override
    void validateVariance(String key, double expected, long cnt, double sum, double variance) {
      assertEquals (key, expected, variance /(cnt-1), 0.0);
    }
  }

  public static class StdValidator extends BaseVarianceValidator {

    @Override
    void validateVariance(String key, double expected, long cnt, double sum, double variance) {
      assertEquals (key, expected, Math.sqrt(variance / cnt), 0.0);
    }
  }

  public static class StdSampValidator extends BaseVarianceValidator {

    @Override
    void validateVariance(String key, double expected, long cnt, double sum, double variance) {
      assertEquals (key, expected, Math.sqrt(variance / (cnt-1)), 0.0);
    }
  }

  private static Object[][] validators = {
      {"count", ValueValidator.class},
      {"min", ValueValidator.class},
      {"max", ValueValidator.class},
      {"sum", ValueValidator.class},
      {"avg", AvgValidator.class},
      {"variance", VarianceValidator.class},
      {"var_pop", VarianceValidator.class},
      {"var_samp", VarianceSampValidator.class},
      {"std", StdValidator.class},
      {"stddev", StdValidator.class},
      {"stddev_pop", StdValidator.class},
      {"stddev_samp", StdSampValidator.class},
  };

  public static Validator getValidator(String aggregate) throws HiveException {
    try
    {
      for (Object[] v: validators) {
        if (aggregate.equalsIgnoreCase((String) v[0])) {
          @SuppressWarnings("unchecked")
          Class<? extends Validator> c = (Class<? extends Validator>) v[1];
          Constructor<? extends Validator> ctr = c.getConstructor();
          return ctr.newInstance();
        }
      }
    }catch(Exception e) {
      throw new HiveException(e);
    }
    throw new HiveException("Missing validator for aggregate: " + aggregate);
  }

  public void testAggregateCountStarIterable (
      Iterable<VectorizedRowBatch> data,
      Object expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("A");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildGroupByDescCountStar (ctx);
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;
    vectorDesc.setProcessingMode(ProcessingMode.HASH);

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator("count");
    validator.validate("_total", expected, result);
  }

  public void testAggregateCountReduceIterable (
      Iterable<VectorizedRowBatch> data,
      Object expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("A");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildGroupByDescType(ctx, "count", GenericUDAFEvaluator.Mode.FINAL, "A", TypeInfoFactory.longTypeInfo);
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;
    vectorDesc.setProcessingMode(ProcessingMode.GLOBAL);  // Use GLOBAL when no key for Reduce.
    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator("count");
    validator.validate("_total", expected, result);
  }

  public void testAggregateStringIterable (
      String aggregateName,
      Iterable<VectorizedRowBatch> data,
      Object expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("A");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildGroupByDescType(ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1, "A",
        TypeInfoFactory.stringTypeInfo);
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator(aggregateName);
    validator.validate("_total", expected, result);
  }

  public void testAggregateDecimalIterable (
          String aggregateName,
          Iterable<VectorizedRowBatch> data,
          Object expected) throws HiveException {
          List<String> mapColumnNames = new ArrayList<String>();
          mapColumnNames.add("A");
          VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair =
        buildGroupByDescType(ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1, "A", TypeInfoFactory.getDecimalTypeInfo(30, 4));
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit : data) {
      vgo.process(unit, 0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator(aggregateName);
    validator.validate("_total", expected, result);
  }


  public void testAggregateDoubleIterable (
      String aggregateName,
      Iterable<VectorizedRowBatch> data,
      Object expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("A");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildGroupByDescType (ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1, "A",
        TypeInfoFactory.doubleTypeInfo);
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator(aggregateName);
    validator.validate("_total", expected, result);
  }

  public void testAggregateLongIterable (
      String aggregateName,
      Iterable<VectorizedRowBatch> data,
      Object expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("A");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildGroupByDescType(ctx, aggregateName, GenericUDAFEvaluator.Mode.PARTIAL1, "A", TypeInfoFactory.longTypeInfo);
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(1, outBatchList.size());

    Object result = outBatchList.get(0);

    Validator validator = getValidator(aggregateName);
    validator.validate("_total", expected, result);
  }

  public void testAggregateLongKeyIterable (
      String aggregateName,
      Iterable<VectorizedRowBatch> data,
      HashMap<Object,Object> expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("Key");
    mapColumnNames.add("Value");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);

    Set<Object> keys = new HashSet<Object>();

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildKeyGroupByDesc (ctx, aggregateName, "Value",
        TypeInfoFactory.longTypeInfo,
        new String[] {"Key"},
        new TypeInfo[] {TypeInfoFactory.longTypeInfo});
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {

      private String aggregateName;
      private HashMap<Object,Object> expected;
      private Set<Object> keys;

      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        assertTrue(row instanceof Object[]);
        Object[] fields = (Object[]) row;
        assertEquals(2, fields.length);
        Object key = fields[0];
        Long keyValue = null;
        if (null != key) {
          assertTrue(key instanceof LongWritable);
          LongWritable lwKey = (LongWritable)key;
          keyValue = lwKey.get();
        }
        assertTrue(expected.containsKey(keyValue));
        String keyAsString = String.format("%s", key);
        Object expectedValue = expected.get(keyValue);
        Object value = fields[1];
        Validator validator = getValidator(aggregateName);
        validator.validate(keyAsString, expectedValue, new Object[] {value});
        keys.add(keyValue);
      }

      private FakeCaptureVectorToRowOutputOperator.OutputInspector init(
          String aggregateName, HashMap<Object,Object> expected, Set<Object> keys) {
        this.aggregateName = aggregateName;
        this.expected = expected;
        this.keys = keys;
        return this;
      }
    }.init(aggregateName, expected, keys));

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(expected.size(), outBatchList.size());
    assertEquals(expected.size(), keys.size());
  }

  public void testAggregateStringKeyIterable (
      String aggregateName,
      Iterable<VectorizedRowBatch> data,
      TypeInfo dataTypeInfo,
      HashMap<Object,Object> expected) throws HiveException {
    List<String> mapColumnNames = new ArrayList<String>();
    mapColumnNames.add("Key");
    mapColumnNames.add("Value");
    VectorizationContext ctx = new VectorizationContext("name", mapColumnNames);
    Set<Object> keys = new HashSet<Object>();

    Pair<GroupByDesc,VectorGroupByDesc> pair = buildKeyGroupByDesc (ctx, aggregateName, "Value",
       dataTypeInfo,
       new String[] {"Key"},
       new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
    GroupByDesc desc = pair.left;
    VectorGroupByDesc vectorDesc = pair.right;

    CompilationOpContext cCtx = new CompilationOpContext();

    Operator<? extends OperatorDesc> groupByOp = OperatorFactory.get(cCtx, desc);

    VectorGroupByOperator vgo =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(groupByOp, ctx, vectorDesc);

    FakeCaptureVectorToRowOutputOperator out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);
    out.setOutputInspector(new FakeCaptureVectorToRowOutputOperator.OutputInspector() {

      private int rowIndex;
      private String aggregateName;
      private HashMap<Object,Object> expected;
      private Set<Object> keys;

      @SuppressWarnings("deprecation")
      @Override
      public void inspectRow(Object row, int tag) throws HiveException {
        assertTrue(row instanceof Object[]);
        Object[] fields = (Object[]) row;
        assertEquals(2, fields.length);
        Object key = fields[0];
        String keyValue = null;
        if (null != key) {
          assertTrue(key instanceof Text);
          Text bwKey = (Text)key;
          keyValue = bwKey.toString();
        }
        assertTrue(expected.containsKey(keyValue));
        Object expectedValue = expected.get(keyValue);
        Object value = fields[1];
        Validator validator = getValidator(aggregateName);
        String keyAsString = String.format("%s", key);
        validator.validate(keyAsString, expectedValue, new Object[] {value});
        keys.add(keyValue);
      }

      private FakeCaptureVectorToRowOutputOperator.OutputInspector init(
          String aggregateName, HashMap<Object,Object> expected, Set<Object> keys) {
        this.aggregateName = aggregateName;
        this.expected = expected;
        this.keys = keys;
        return this;
      }
    }.init(aggregateName, expected, keys));

    for (VectorizedRowBatch unit: data) {
      vgo.process(unit,  0);
    }
    vgo.close(false);

    List<Object> outBatchList = out.getCapturedRows();
    assertNotNull(outBatchList);
    assertEquals(expected.size(), outBatchList.size());
    assertEquals(expected.size(), keys.size());
  }

  @Test
  public void testSingleBytesKeyModeSelection() throws HiveException {
    // BytesKeyGroupBy asserts the mode of each bytes key type.
    assertEquals(VectorGroupByOperator.ProcessingModeHashAggregateSingleLongKey.class,
        hashProcessingModeClass(new String[] {"k1"},
            new TypeInfo[] {TypeInfoFactory.longTypeInfo}));
    assertEquals(VectorGroupByOperator.ProcessingModeHashAggregateSingleLongKey.class,
        hashProcessingModeClass(new String[] {"k1"},
            new TypeInfo[] {TypeInfoFactory.dateTypeInfo}));
    assertEquals(VectorGroupByOperator.ProcessingModeHashAggregateSerializedKey.class,
        hashProcessingModeClass(new String[] {"k1", "k2"},
            new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo}));
    assertEquals(VectorGroupByOperator.ProcessingModeHashAggregateSerializedKey.class,
        hashProcessingModeClass(new String[] {"k1", "k2"},
            new TypeInfo[] {TypeInfoFactory.dateTypeInfo, TypeInfoFactory.getCharTypeInfo(10)}));
    assertEquals(VectorGroupByOperator.ProcessingModeHashAggregate.class,
        hashProcessingModeClass(new String[] {"k1", "k2"},
            new TypeInfo[] {TypeInfoFactory.longTypeInfo, TypeInfoFactory.doubleTypeInfo}));
    assertEquals(VectorGroupByOperator.ProcessingModeHashAggregate.class,
        hashProcessingModeClass(new String[] {"k1"},
            new TypeInfo[] {TypeInfoFactory.getDecimalTypeInfo(10, 2)}));
    assertEquals(VectorGroupByOperator.ProcessingModeHashAggregate.class,
        hashProcessingModeClass(new String[] {"k1"},
            new TypeInfo[] {TypeInfoFactory.longTypeInfo}, desc -> {
              desc.setGroupingSetsPresent(true);
              desc.setListGroupingSets(new ArrayList<>(Arrays.asList(0L, 1L)));
              desc.getKeys().add(new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 0L));
              desc.setGroupingSetPosition(1);
            }));
    assertEquals(VectorGroupByOperator.ProcessingModeHashAggregate.class,
        hashProcessingModeClass(new String[] {"k1"},
            new TypeInfo[] {TypeInfoFactory.stringTypeInfo}, desc -> {
              desc.setGroupingSetsPresent(true);
              desc.setListGroupingSets(new ArrayList<>(Arrays.asList(0L, 1L)));
              desc.getKeys().add(new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 0L));
              desc.setGroupingSetPosition(1);
            }));
  }

  private Class<?> hashProcessingModeClass(String[] keys, TypeInfo[] keyTypeInfos)
      throws HiveException {
    return hashProcessingModeClass(keys, keyTypeInfos, desc -> { });
  }

  private Class<?> hashProcessingModeClass(String[] keys, TypeInfo[] keyTypeInfos,
      Consumer<GroupByDesc> descSetup) throws HiveException {
    List<String> columnNames = new ArrayList<>(Arrays.asList(keys));
    columnNames.add("v");
    VectorizationContext ctx = new VectorizationContext("name", columnNames);
    Pair<GroupByDesc, VectorGroupByDesc> pair = buildKeyGroupByDesc(ctx, "count", "v",
        TypeInfoFactory.longTypeInfo, keys, keyTypeInfos);
    descSetup.accept(pair.left);
    CompilationOpContext cCtx = new CompilationOpContext();
    VectorGroupByOperator vgo = (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(
        OperatorFactory.get(cCtx, pair.left), ctx, pair.right);
    FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
    vgo.initialize(hconf, null);
    return ((Object) vgo.processingMode).getClass();
  }

  @Test
  public void testSingleBytesKeyNullAndEmpty() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      // NULL is the 16th key, which fills the initial capacity of the table, followed by the empty
      // key, which has zero words and hash, like the unset record of the NULL entry. More keys grow
      // the table again.
      byte[][] keys = new byte[40][];
      keys[16] = bytes("");
      for (int i = 0; i < keys.length; i++) {
        if (i != 15 && i != 16) {
          keys[i] = bytes("k" + i);
        }
      }
      Map<String, List<Long>> expected = new HashMap<>();
      BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, aggregationBuffers);
      gby.process(bytesKeyBatch(keys, false, null, expected));
      gby.process(bytesKeyBatch(keys(null, "", "", null), false, null, expected));
      assertEquals(40, gby.mode().keyTable.size());
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSingleBytesKeyNullFlushedAndAddedAgain() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      byte[][] keys = new byte[100][];
      for (int i = 1; i < keys.length; i++) {
        keys[i] = bytes("k" + i);
      }
      Map<String, List<Long>> expected = new HashMap<>();
      BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, aggregationBuffers);
      gby.process(bytesKeyBatch(keys, false, null, expected));
      // The partial flush emits the 10 oldest entries, NULL first.
      gby.mode().gcCanary.clear();
      gby.process(bytesKeyBatch(keys("k50"), false, null, expected));
      assertEquals(90, gby.mode().keyTable.size());
      // NULL and k1 get new entries, k1 probing past the slot of its removed entry.
      gby.process(bytesKeyBatch(keys(null, null, "k1"), false, null, expected));
      assertEquals(92, gby.mode().keyTable.size());
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSingleBytesKeyRepeating() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      Map<String, List<Long>> expected = new HashMap<>();
      BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, aggregationBuffers);
      gby.process(repeatingBytesKeyBatch(bytes("a"), 7, null, expected));
      gby.process(bytesKeyBatch(keys("a", "b", null), false, null, expected));
      gby.process(repeatingBytesKeyBatch(bytes("b"), 5, new int[] {1, 3}, expected));
      gby.process(repeatingBytesKeyBatch(null, 4, null, expected));
      gby.process(repeatingBytesKeyBatch(null, 6, new int[] {0, 5}, expected));
      gby.process(repeatingBytesKeyBatch(bytes("c"), 3, null, expected));
      assertEquals(4, expected.size());
      // One access per row, except for the row that adds the key.
      assertEquals(6 + 1, gby.accessCount(0));
      assertEquals(2, gby.accessCount(1));
      assertEquals(4 + 2, gby.accessCount(2));
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSingleBytesKeySelectedInUse() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      Map<String, List<Long>> expected = new HashMap<>();
      BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, aggregationBuffers);
      gby.process(bytesKeyBatch(keys("a", "x1", "b", "x2", null, "a"), false, new int[] {0, 2, 4, 5},
          expected));
      gby.process(bytesKeyBatch(keys("x3", "b", "x4", null), false, new int[] {1, 3}, expected));
      assertEquals(3, expected.size());
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSingleBytesKeyCharVarcharBinary() throws HiveException {
    for (TypeInfo keyTypeInfo : new TypeInfo[] {TypeInfoFactory.getCharTypeInfo(10),
        TypeInfoFactory.getVarcharTypeInfo(10), TypeInfoFactory.binaryTypeInfo}) {
      Map<String, List<Long>> expected = new HashMap<>();
      BytesKeyGroupBy gby = new BytesKeyGroupBy(keyTypeInfo);
      byte[][] keys = keyTypeInfo == TypeInfoFactory.binaryTypeInfo
          ? new byte[][] {{0}, {0, 0}, {(byte) 0xFF, 1}, null, {}, {0}}
          : keys("ab", "abc", "ab c", null, "abcdefghij", "ab");
      gby.process(bytesKeyBatch(keys, false, null, expected));
      gby.process(bytesKeyBatch(keys, true, null, expected));
      assertEquals(keyTypeInfo.toString(), 5, expected.size());
      assertEquals(keyTypeInfo.toString(), expected, gby.close());
    }
  }

  @Test
  public void testSingleBytesKeyLengths() throws HiveException {
    // Keys of 0 to 40 bytes that share their prefixes, differ in their last byte or in a trailing
    // zero byte, referenced both inside a larger buffer and as whole arrays.
    String chars = "abcdefghijklmnopqrstuvwxyz0123456789ABCDE";
    List<byte[]> keyList = new ArrayList<>();
    for (int length = 0; length <= 40; length++) {
      keyList.add(bytes(chars.substring(0, length)));
      keyList.add(bytes(chars.substring(0, length) + "\0"));
      if (length > 0) {
        keyList.add(bytes(chars.substring(0, length - 1) + "_"));
      }
    }
    byte[][] keys = keyList.toArray(new byte[0][]);
    Map<String, List<Long>> expected = new HashMap<>();
    BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo);
    gby.process(bytesKeyBatch(keys, false, null, expected));
    gby.process(bytesKeyBatch(keys, true, null, expected));
    assertEquals(keys.length, expected.size());
    assertEquals(expected, gby.close());
  }

  @Test
  public void testSingleBytesKeyHashCollisions() throws HiveException {
    // Pairs of keys with the same hash and length that differ in the first word, the second word
    // or the bytes after them.
    List<byte[]> keyList = new ArrayList<>();
    for (String format :
        new String[] {"%08d", "%08d01234567", "01234567%08d", "0123456789abcdef%08d"}) {
      keyList.addAll(Arrays.asList(collidingKeys(format)));
    }
    byte[][] keys = keyList.toArray(new byte[0][]);
    Map<String, List<Long>> expected = new HashMap<>();
    BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo);
    gby.process(bytesKeyBatch(keys, false, null, expected));
    gby.process(bytesKeyBatch(keys, true, null, expected));
    assertEquals(8, expected.size());
    assertEquals(expected, gby.close());
  }

  /**
   * Returns two keys of the format, which has a single int argument, with the same hash.
   */
  private static byte[][] collidingKeys(String format) {
    Map<Integer, byte[]> keysByHash = new HashMap<>();
    for (int i = 0; ; i++) {
      byte[] key = bytes(String.format(format, i));
      byte[] other = keysByHash.put(VectorGroupByBytesKeyTable.hash(key, 0, key.length), key);
      if (other != null) {
        return new byte[][] {other, key};
      }
    }
  }

  @Test
  public void testSingleBytesKeyManyKeys() throws HiveException {
    Map<String, List<Long>> expected = new HashMap<>();
    BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo);
    final int keyCount = 100_000;
    for (int pass = 0; pass < 2; pass++) {
      for (int first = 0; first < keyCount; first += VectorizedRowBatch.DEFAULT_SIZE) {
        byte[][] keys = new byte[Math.min(VectorizedRowBatch.DEFAULT_SIZE, keyCount - first)][];
        for (int i = 0; i < keys.length; i++) {
          int key = pass == 0 ? first + i : keyCount - 1 - first - i;
          keys[i] = bytes(key % 2 == 0 ? "key-" + key : "a-longer-key-than-16-bytes-" + key);
        }
        gby.process(bytesKeyBatch(keys, false, null, expected));
      }
    }
    assertEquals(keyCount, gby.mode().keyTable.size());
    assertEquals(keyCount, expected.size());
    assertEquals(expected, gby.close());
  }

  @Test
  public void testSingleBytesKeyPartialFlush() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      // 100 hot keys accessed 11 times, then 900 cold keys, including NULL, accessed once. A partial
      // flush emits 10% of the entries, the oldest ones accessed at most as often as average.
      byte[][] hot = new byte[100][];
      for (int i = 0; i < hot.length; i++) {
        hot[i] = bytes("hot-" + i);
      }
      byte[][] cold = new byte[900][];
      for (int i = 0; i < cold.length; i++) {
        cold[i] = i == 500 ? null : bytes("cold-" + i);
      }
      byte[][] all = new byte[hot.length + cold.length][];
      System.arraycopy(hot, 0, all, 0, hot.length);
      System.arraycopy(cold, 0, all, hot.length, cold.length);
      byte[][] hotTenTimes = new byte[10 * hot.length][];
      for (int i = 0; i < hotTenTimes.length; i++) {
        hotTenTimes[i] = hot[i % hot.length];
      }

      Map<String, List<Long>> expected = new HashMap<>();
      BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, aggregationBuffers);
      gby.process(bytesKeyBatch(all, false, null, expected));
      gby.process(bytesKeyBatch(all, false, null, expected));
      gby.mode().gcCanary.clear();
      gby.process(bytesKeyBatch(hotTenTimes, false, null, expected));
      assertEquals(900, gby.mode().keyTable.size());
      gby.process(bytesKeyBatch(all, false, null, expected));

      assertEquals(expected, gby.close());
      List<Object> rows = gby.out.getCapturedRows();
      assertEquals(100 + 1000, rows.size());
      for (int i = 0; i < 100; i++) {
        assertEquals("cold-" + i, keyString(((Object[]) rows.get(i))[0]));
      }
    }
  }

  @Test
  public void testSingleBytesKeyPartialFlushKeepsCapacity() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      // 33 keys grow the table to a capacity of 64, which it keeps after flushing and compacting 19
      // of them.
      hconf.setFloat(HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_FLUSH_PERCENT.varname, 0.6f);
      byte[][] keys = new byte[33][];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = bytes(String.format("key-%04d", i));
      }
      long keyMemorySize = JavaDataModel.get().lengthForByteArrayOfSize(8);
      Map<String, List<Long>> expected = new HashMap<>();
      BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, aggregationBuffers);
      VectorGroupByBytesKeyTable table = gby.mode().keyTable;
      gby.process(bytesKeyBatch(keys, false, null, expected));
      long arraysMemorySize = table.getMemorySize() - table.size() * keyMemorySize;
      gby.mode().gcCanary.clear();
      gby.process(bytesKeyBatch(keys("key-0000"), false, null, expected));
      assertEquals(14, table.size());
      assertEquals(14, table.end());
      assertEquals(arraysMemorySize, table.getMemorySize() - table.size() * keyMemorySize);
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSingleBytesKeyRemovedEntriesKeepCapacity() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      // 40 keys fill 0.6 of a capacity of 64. Each round adds 4 keys and flushes 4, which the table
      // compacts once they fill its free room instead of growing for them.
      byte[][] keys = new byte[40][];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = bytes(String.format("key-%04d", i));
      }
      long keyMemorySize = JavaDataModel.get().lengthForByteArrayOfSize(8);
      Map<String, List<Long>> expected = new HashMap<>();
      BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, aggregationBuffers);
      VectorGroupByBytesKeyTable table = gby.mode().keyTable;
      gby.process(bytesKeyBatch(keys, false, null, expected));
      long arraysMemorySize = table.getMemorySize() - table.size() * keyMemorySize;
      for (int round = 0; round < 20; round++) {
        byte[][] added = new byte[4][];
        for (int i = 0; i < added.length; i++) {
          added[i] = bytes(String.format("key-%04d", keys.length + round * added.length + i));
        }
        gby.mode().gcCanary.clear();
        gby.process(bytesKeyBatch(added, false, null, expected));
        assertEquals(40, table.size());
        assertEquals(arraysMemorySize, table.getMemorySize() - table.size() * keyMemorySize);
      }
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSingleBytesKeyFullTableGrowsOnCompaction() throws HiveException {
    // About 110 keys in a capacity of 128. Each round adds 11 keys and flushes 10% of the entries,
    // which leaves too little free room: the first compaction doubles the capacity, so the next
    // ones follow only after several rounds rather than on every one.
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      Map<String, List<Long>> expected = new HashMap<>();
      BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, aggregationBuffers);
      VectorGroupByBytesKeyTable table = gby.mode().keyTable;
      byte[][] keys = new byte[110][];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = bytes(String.format("key-%04d", i));
      }
      gby.process(bytesKeyBatch(keys, false, null, expected));
      int compactions = 0;
      for (int round = 0; round < 20; round++) {
        byte[][] added = new byte[11][];
        for (int i = 0; i < added.length; i++) {
          added[i] = bytes(String.format("key-%04d", keys.length + round * added.length + i));
        }
        int end = table.end();
        gby.mode().gcCanary.clear();
        gby.process(bytesKeyBatch(added, false, null, expected));
        if (table.end() < end + added.length) {
          compactions++;
        }
      }
      assertTrue(compactions + " compactions", compactions > 0 && compactions <= 5);
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSingleBytesKeySmallPartialFlushes() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      // With flush.percent 0.001 each flush removes the oldest of 1100 entries, NULL first, in a
      // table of capacity 2048, which compacts once the removed entries outnumber the others.
      hconf.setFloat(HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_FLUSH_PERCENT.varname, 0.001f);
      Map<String, List<Long>> expected = new HashMap<>();
      BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, aggregationBuffers);
      VectorGroupByBytesKeyTable table = gby.mode().keyTable;
      for (int first = 0; first < 1100; first += 550) {
        byte[][] keys = new byte[550][];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = first + i == 0 ? null : bytes("k" + (first + i));
        }
        gby.process(bytesKeyBatch(keys, false, null, expected));
      }
      int compactions = 0;
      int maxRemoved = 0;
      for (int batch = 0; batch < 700; batch++) {
        int end = table.end();
        gby.mode().gcCanary.clear();
        // NULL is added again once, as the newest entry.
        gby.process(bytesKeyBatch(keys(batch == 300 ? null : "k1099"), false, null, expected));
        int removed = table.end() - table.size();
        assertTrue(removed + " removed of " + table.end(), removed <= table.size());
        maxRemoved = Math.max(maxRemoved, removed);
        if (table.end() < end) {
          compactions++;
        }
      }
      assertEquals(1, compactions);
      assertTrue(maxRemoved + " removed", maxRemoved >= 500);
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSingleBytesKeyMemoryFlush() throws HiveException {
    // Keys of 1000 bytes and a 100KB limit keep about 80 entries. Each batch adds 10 keys, so once
    // the table is full, each batch flushes and the new entries reuse the flushed buffers.
    long maxMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    float memoryUsage = 100.0f * 1024.0f / maxMemory;
    long maxHashTableMemory = (int) (maxMemory * memoryUsage);
    BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, true,
        desc -> desc.setGroupByMemoryUsage(memoryUsage));
    VectorAggregationBufferBatch aggregationBatch = new VectorAggregationBufferBatch();
    aggregationBatch.compileAggregationBatchInfo(gby.vgo.aggregators);
    VectorAggregateExpression aggregator = spy(gby.vgo.aggregators[0]);
    gby.vgo.aggregators[0] = aggregator;
    VectorGroupByBytesKeyTable table = gby.mode().keyTable;
    Map<String, List<Long>> expected = new HashMap<>();
    char[] padding = new char[990];
    Arrays.fill(padding, 'x');
    int flushes = 0;
    int maxEntries = 0;
    for (int batch = 0; batch < 50; batch++) {
      int entries = table.size();
      byte[][] keys = new byte[10][];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = bytes(String.format("%010d", batch * keys.length + i) + new String(padding));
      }
      gby.process(bytesKeyBatch(keys, false, null, expected));
      maxEntries = Math.max(maxEntries, entries + keys.length);
      // The table is estimated, the aggregation buffers have a fixed size.
      assertEquals(table.size() * aggregationBatch.getAggregatorsFixedSize()
          + table.getMemorySize(), gby.mode().getHashTableMemorySize());
      assertTrue(gby.mode().getHashTableMemorySize() <= maxHashTableMemory);
      if (table.size() < entries + keys.length) {
        flushes++;
        assertTrue(table.size() + " entries after a flush", table.size() >= 50);
      }
    }
    assertTrue(flushes + " flushes", flushes >= 30);
    verify(aggregator, times(maxEntries)).getNewAggregationBuffer();
    assertEquals(expected, gby.close());
  }

  @Test
  public void testSingleBytesKeySwitchToStreaming() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      hconf.set("hive.groupby.mapaggr.checkinterval", "1000");
      hconf.set("hive.vectorized.groupby.maxentries", "100");
      BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, aggregationBuffers,
          desc -> desc.setMinReductionHashAggr(0.4f));
      Map<String, List<Long>> expected = new HashMap<>();
      for (int batch = 0; batch < 5; batch++) {
        byte[][] keys = new byte[VectorizedRowBatch.DEFAULT_SIZE][];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = bytes("key-" + (batch * keys.length + i) / 2);
        }
        gby.process(bytesKeyBatch(keys, false, null, expected));
      }
      assertTrue(gby.vgo.processingMode instanceof VectorGroupByOperator.ProcessingModeStreaming);
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSerializedKeyNullsAndLengths() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      // Pairs of keys that differ only by a NULL, by which LONG value takes 8 bytes or by where
      // one BYTES value ends.
      Object[][] longLong = {
          {null, 0L, 1L, 1L, null, 0L, Long.MIN_VALUE, -1L, Long.MAX_VALUE, 1L, 0x2_0000_0001L},
          {1L, 1L, null, 0L, null, 0L, Long.MAX_VALUE, -1L, Long.MIN_VALUE, 0x3_0000_0002L, 3L}};
      Object[][] bytesBytes = {
          {"ab", "a", null, "", "a", "a", null, "", "x\0", "x"},
          {"c", "bc", "a", "a", null, "", null, "", "", "\0"}};
      Object[][] longBytes = {
          {1L, null, 0L, 1L, 1L, null},
          {"x", "x", "x", null, "", null}};
      Object[][] bytesLongBytes = {
          {"a", "ab", "a", null, ""},
          {7L, 7L, null, 7L, 7L},
          {"bc", "c", "bc", "abc", "abc"}};
      for (Object[][] columns : new Object[][][] {longLong, bytesBytes, longBytes, bytesLongBytes}) {
        Map<List<Object>, List<Long>> expected = new HashMap<>();
        KeyTableGroupBy gby = new KeyTableGroupBy(keyTypeInfos(columns), aggregationBuffers);
        gby.process(multiKeyBatch(columns, null, expected));
        gby.process(multiKeyBatch(columns, new int[] {0, 2, 3}, expected));
        assertEquals(columns[0].length, gby.mode().keyTable.size());
        assertEquals(expected, gby.close());
      }
    }
  }

  @Test
  public void testSingleLongKey() throws HiveException {
    long[] colliding = collidingLongKeys();
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      // NULL and 0, keys of equal hash, and repeating keys.
      Object[][] columns =
          {{0L, null, 5L, null, 0L, -1L, 1L << 32, Long.MIN_VALUE, colliding[0], colliding[1]}};
      Map<List<Object>, List<Long>> expected = new HashMap<>();
      KeyTableGroupBy gby = new KeyTableGroupBy(keyTypeInfos(columns), aggregationBuffers);
      gby.process(multiKeyBatch(columns, null, expected));
      gby.process(multiKeyBatch(columns, new int[] {1, 2, 9}, expected));
      for (Object first : new Object[] {7L, null}) {
        VectorizedRowBatch batch = multiKeyBatch(new Object[][] {{first, 8L, 0L}}, null, null);
        repeating(batch.cols[0]);
        addExpected(batch, 1, expected);
        gby.process(batch);
      }
      if (aggregationBuffers) {
        // The row that added the entry is not an access.
        VectorGroupByBytesKeyTable table = gby.mode().keyTable;
        assertEquals(2, table.getRow(table.findOrAdd(7L)).getAccessCount());
      }
      assertEquals(9, gby.mode().keyTable.size());
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSingleLongKeyMemory() {
    // LONG keys have no key bytes: only the table arrays count.
    VectorGroupByBytesKeyTable table = new VectorGroupByBytesKeyTable();
    long arrays = table.getMemorySize();
    for (long key = 0; key < 10; key++) {
      table.findOrAdd(key);
    }
    assertEquals(arrays, table.getMemorySize());
    for (int entry = 0; entry < 5; entry++) {
      table.remove(entry);
    }
    assertEquals(arrays, table.getMemorySize());
  }

  /**
   * Returns two LONG keys that have the same hash in the table.
   */
  private static long[] collidingLongKeys() {
    Map<Integer, Long> keysByHash = new HashMap<>();
    byte[] bytes = new byte[Long.BYTES];
    for (long key = 1000; ; key++) {
      for (int i = 0; i < Long.BYTES; i++) {
        bytes[i] = (byte) (key >>> (Byte.SIZE * i));
      }
      Long other = keysByHash.put(VectorGroupByBytesKeyTable.hash(bytes, 0, Long.BYTES), key);
      if (other != null) {
        return new long[] {other, key};
      }
    }
  }

  @Test
  public void testSerializedKeyRepeatingSelected() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      Map<List<Object>, List<Long>> expected = new HashMap<>();
      KeyTableGroupBy gby = new KeyTableGroupBy(new TypeInfo[] {
          TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo,
          TypeInfoFactory.longTypeInfo}, aggregationBuffers);
      Object[][] columns = {
          {3L, 4L, 3L, 4L},
          {"a", "b", null, "a"},
          {null, null, null, null}};
      for (int[] selected : new int[][] {null, {1, 3}}) {
        for (int repeating = 0; repeating < 8; repeating++) {
          VectorizedRowBatch batch = multiKeyBatch(columns, selected, null);
          for (int c = 0; c < columns.length; c++) {
            if ((repeating & (1 << c)) != 0) {
              repeating(batch.cols[c]);
            }
          }
          addExpected(batch, columns.length, expected);
          gby.process(batch);
        }
      }
      // (3, a), (4, b), (3, NULL), (4, a), and (3, b) when only the first column repeats.
      assertEquals(5, gby.mode().keyTable.size());
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSerializedKeyRandom() throws HiveException {
    // Random keys of four columns, with NULLs, LONG values of 4 and 8 bytes, repeating columns and
    // selected rows, through the partial flushes of a small memory limit.
    Random random = new Random(41);
    TypeInfo[] typeInfos = {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo,
        TypeInfoFactory.getVarcharTypeInfo(20), TypeInfoFactory.intTypeInfo};
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      hconf.set("hive.vectorized.groupby.maxentries", "100");
      hconf.set("hive.vectorized.groupby.checkinterval", "10");
      KeyTableGroupBy gby = new KeyTableGroupBy(typeInfos, aggregationBuffers,
          desc -> desc.setGroupByMemoryUsage(0.0001f));
      Map<List<Object>, List<Long>> expected = new HashMap<>();
      for (int b = 0; b < 200; b++) {
        int size = 1 + random.nextInt(VectorizedRowBatch.DEFAULT_SIZE);
        Object[][] columns = new Object[typeInfos.length][size];
        for (int i = 0; i < size; i++) {
          columns[0][i] = random.nextInt(10) == 0 ? null : "s" + random.nextInt(30);
          columns[1][i] = random.nextInt(10) == 0 ? null
              : ((long) random.nextInt(5) - 2) << (random.nextBoolean() ? 0 : 33);
          columns[2][i] = random.nextInt(10) == 0 ? null
              : "x".repeat(random.nextInt(4)) + random.nextInt(3);
          columns[3][i] = random.nextInt(10) == 0 ? null : (long) random.nextInt(4);
        }
        int[] selected = null;
        if (random.nextBoolean()) {
          selected = random.ints(0, size).distinct().limit(1 + random.nextInt(size)).sorted()
              .toArray();
        }
        VectorizedRowBatch batch = multiKeyBatch(columns, selected, null);
        for (int c = 0; c < columns.length; c++) {
          if (random.nextInt(8) == 0) {
            repeating(batch.cols[c]);
          }
        }
        addExpected(batch, columns.length, expected);
        gby.process(batch);
      }
      assertTrue(gby.out.getCapturedRows().size() + " rows before close",
          gby.out.getCapturedRows().size() > 1000);
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testSerializedKeySwitchToStreaming() throws HiveException {
    for (boolean aggregationBuffers : new boolean[] {false, true}) {
      hconf.set("hive.groupby.mapaggr.checkinterval", "1000");
      hconf.set("hive.vectorized.groupby.maxentries", "100");
      KeyTableGroupBy gby = new KeyTableGroupBy(new TypeInfo[] {
          TypeInfoFactory.longTypeInfo, TypeInfoFactory.stringTypeInfo}, aggregationBuffers,
          desc -> desc.setMinReductionHashAggr(0.4f));
      Map<List<Object>, List<Long>> expected = new HashMap<>();
      for (int b = 0; b < 5; b++) {
        Object[][] columns = new Object[2][VectorizedRowBatch.DEFAULT_SIZE];
        for (int i = 0; i < columns[0].length; i++) {
          int key = (b * columns[0].length + i) / 2;
          columns[0][i] = (long) key;
          columns[1][i] = "key-" + key % 7;
        }
        gby.process(multiKeyBatch(columns, null, expected));
      }
      assertTrue(gby.vgo.processingMode instanceof VectorGroupByOperator.ProcessingModeStreaming);
      assertEquals(expected, gby.close());
    }
  }

  @Test
  public void testAggregationColumnsCoverage() throws HiveException {
    AggregationColumnsGroupBy gby = new AggregationColumnsGroupBy();
    List<String> classes = new ArrayList<>();
    for (VectorAggregateExpression aggregator : gby.columns.aggregators) {
      classes.add(aggregator.getClass().getSimpleName());
    }
    assertEquals(Arrays.asList("VectorUDAFMinLong", "VectorUDAFMaxLong", "VectorUDAFSumLong",
        "VectorUDAFCount", "VectorUDAFMinDecimal64", "VectorUDAFMaxDecimal64",
        "VectorUDAFSumDecimal64", "VectorUDAFCount", "VectorUDAFCountStar"), classes);
    assertNotNull(AggregationColumnsGroupBy.mode(gby.columns).aggregationColumns);
    // The avg of the oracle is not covered.
    assertEquals(null, AggregationColumnsGroupBy.mode(gby.buffers).aggregationColumns);
    assertNotNull(VectorGroupByAggregationColumns.create(new VectorAggregateExpression[0]));
  }

  @Test
  public void testAggregationColumnsNullsRepeatingSelected() throws HiveException {
    AggregationColumnsGroupBy gby = new AggregationColumnsGroupBy();
    // n has only NULL values, the NULL key only a NULL d.
    gby.process(aggregationBatch(6, null,
        keyColumn("a", "b", "a", "c", null, "n"),
        longColumn(1L, null, 5L, null, 7L, null),
        repeating(decimal64Column((Long) null))));
    gby.process(aggregationBatch(3, null,
        repeating(keyColumn("b")),
        repeating(longColumn(9L)),
        decimal64Column(100L, -200L, 300L)));
    // x is not selected; a gets the LONG bounds that the DECIMAL_64 min and max start from.
    gby.process(aggregationBatch(5, new int[] {0, 1, 2, 4},
        keyColumn("a", "c", "c", "x", "a"),
        longColumn(Long.MAX_VALUE, -3L, 4L, 1000L, Long.MIN_VALUE),
        repeating(decimal64Column(50L))));
    gby.process(aggregationBatch(2, null,
        keyColumn("n", "n"),
        repeating(longColumn((Long) null)),
        decimal64Column(null, null)));
    assertEquals(Arrays.asList(
        // min(l), max(l), sum(l), count(l), min(d), max(d), sum(d), count(d), count(*)
        "a," + Long.MIN_VALUE + "," + Long.MAX_VALUE + "," + (1 + 5 + Long.MAX_VALUE + Long.MIN_VALUE)
            + ",4,50,50,100,2,4",
        "b,9,9,27,3,-200,300,200,3,4",
        "c,-3,4,1,2,50,50,100,2,3",
        "NULL,7,7,7,1,NULL,NULL,NULL,0,1",
        "n,NULL,NULL,NULL,0,NULL,NULL,NULL,0,3"), gby.close());
  }

  @Test
  public void testAggregationColumnsDecimal64SumOverflow() throws HiveException {
    // sum(decimal(8,2)) is decimal(18,2): a sum beyond 18 digits at any row is NULL for good.
    final long big = 600_000_000_000_000_000L;
    AggregationColumnsGroupBy gby = new AggregationColumnsGroupBy();
    gby.process(aggregationBatch(6, null,
        keyColumn("a", "a", "b", "b", "b", "c"),
        longColumn(0L, 0L, 0L, 0L, 0L, 0L),
        decimal64Column(big, big, big, big, -2 * big, 5L)));
    gby.process(aggregationBatch(3, null,
        repeating(keyColumn("a")),
        repeating(longColumn(0L)),
        decimal64Column(-big, -big, 1L)));
    gby.process(aggregationBatch(3, new int[] {0, 2},
        keyColumn("s", "c", "s"),
        repeating(longColumn(0L)),
        decimal64Column(big, 1L, big / 2)));
    gby.process(aggregationBatch(4, null,
        repeating(keyColumn("r")),
        repeating(longColumn(0L)),
        repeating(decimal64Column(big / 2))));
    List<String> rows = gby.close();
    assertEquals(Arrays.asList("a", "b", "c", "s", "r"),
        rows.stream().map(row -> row.split(",")[0]).collect(Collectors.toList()));
    List<String> sums = rows.stream().map(row -> row.split(",")[7]).collect(Collectors.toList());
    assertEquals(Arrays.asList("NULL", "NULL", "5", String.valueOf(big + big / 2), "NULL"), sums);
  }

  @Test
  public void testAggregationColumnsCompaction() throws HiveException {
    // As in testSingleBytesKeySmallPartialFlushes, each flush removes the oldest entry until the
    // table compacts, which moves the entries, each with its own values, and their access counts.
    hconf.setFloat(HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_FLUSH_PERCENT.varname, 0.001f);
    AggregationColumnsGroupBy gby = new AggregationColumnsGroupBy();
    for (int first = 0; first < 1100; first += 550) {
      String[] keys = new String[550];
      Long[] values = new Long[550];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = first + i == 0 ? null : "k" + (first + i);
        values[i] = (long) first + i;
      }
      gby.process(aggregationBatch(keys.length, null, keyColumn(keys), longColumn(values),
          decimal64Column(values)));
    }
    for (int batch = 0; batch < 700; batch++) {
      gby.flushOnNextBatch();
      String key = batch % 3 == 0 ? "k" + (1099 - batch % 100) : "k" + (500 + batch % 200);
      gby.process(aggregationBatch(2, null, keyColumn(key, "k1099"), longColumn((long) -batch, (long) batch),
          repeating(decimal64Column((long) batch))));
    }
    assertEquals(1, gby.compactions);
    gby.close();
  }

  @Test
  public void testAggregationColumnsMatchAggregationBuffers() throws HiveException {
    // Random batches of every shape, with partial flushes of several sizes, give the same output
    // rows, in the same order, as the aggregation buffers.
    int compactions = 0;
    for (float flushPercent : new float[] {0.1f, 0.5f, 0.01f}) {
      for (int seed = 0; seed < 3; seed++) {
        hconf.setFloat(HiveConf.ConfVars.HIVE_VECTORIZATION_GROUPBY_FLUSH_PERCENT.varname,
            flushPercent);
        Random random = new Random(seed);
        AggregationColumnsGroupBy gby = new AggregationColumnsGroupBy();
        for (int batch = 0; batch < 300; batch++) {
          if (random.nextInt(4) == 0) {
            gby.flushOnNextBatch();
          }
          gby.process(randomAggregationBatch(random, 3000));
        }
        compactions += gby.compactions;
        assertTrue(gby.flushes + " flushes", gby.flushes > 50);
        gby.close();
      }
    }
    assertTrue(compactions + " compactions", compactions > 0);
  }

  @Test
  public void testAggregationColumnsMemoryFlush() throws HiveException {
    // As testSingleBytesKeyMemoryFlush, with the memory of the aggregation columns estimated from
    // their arrays and no aggregation buffers allocated.
    long maxMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    float memoryUsage = 100.0f * 1024.0f / maxMemory;
    long maxHashTableMemory = (int) (maxMemory * memoryUsage);
    BytesKeyGroupBy gby = new BytesKeyGroupBy(TypeInfoFactory.stringTypeInfo, false,
        desc -> desc.setGroupByMemoryUsage(memoryUsage));
    VectorAggregateExpression aggregator = spy(gby.vgo.aggregators[0]);
    gby.vgo.aggregators[0] = aggregator;
    VectorGroupByBytesKeyTable table = gby.mode().keyTable;
    VectorGroupByAggregationColumns columns = gby.mode().aggregationColumns;
    Map<String, List<Long>> expected = new HashMap<>();
    char[] padding = new char[990];
    Arrays.fill(padding, 'x');
    int flushes = 0;
    for (int batch = 0; batch < 50; batch++) {
      int entries = table.size();
      byte[][] keys = new byte[10][];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = bytes(String.format("%010d", batch * keys.length + i) + new String(padding));
      }
      gby.process(bytesKeyBatch(keys, false, null, expected));
      assertEquals(table.getMemorySize() + columns.getMemorySize(),
          gby.mode().getHashTableMemorySize());
      assertTrue(gby.mode().getHashTableMemorySize() <= maxHashTableMemory);
      if (table.size() < entries + keys.length) {
        flushes++;
        assertTrue(table.size() + " entries after a flush", table.size() >= 50);
      }
    }
    assertTrue(flushes + " flushes", flushes >= 30);
    verify(aggregator, times(0)).getNewAggregationBuffer();
    assertEquals(expected, gby.close());
  }

  private static final TypeInfo DECIMAL_8_2 = TypeInfoFactory.getDecimalTypeInfo(8, 2);

  /**
   * Returns a batch of up to 1024 rows of keys out of keyCount keys, with every shape of the key
   * and value columns: repeating or not, with or without NULLs, selected or not, with LONG bounds
   * and DECIMAL_64 values that make a sum overflow.
   */
  private static VectorizedRowBatch randomAggregationBatch(Random random, int keyCount) {
    int size = random.nextInt(8) == 0 ? random.nextInt(3) : 1 + random.nextInt(1024);
    String[] keys = new String[size];
    Long[] longs = new Long[size];
    Long[] decimals = new Long[size];
    boolean keyNulls = random.nextBoolean();
    boolean longNulls = random.nextInt(3) == 0;
    boolean decimalNulls = random.nextInt(3) == 0;
    long decimalRange = random.nextInt(10) == 0 ? 400_000_000_000_000_000L : 99_999_999L;
    for (int i = 0; i < size; i++) {
      keys[i] = keyNulls && random.nextInt(20) == 0 ? null : "key-" + random.nextInt(keyCount);
      longs[i] = longNulls && random.nextInt(3) == 0 ? null
          : random.nextInt(50) == 0 ? (random.nextBoolean() ? Long.MAX_VALUE : Long.MIN_VALUE)
          : (long) random.nextInt(2001) - 1000;
      decimals[i] = decimalNulls && random.nextInt(3) == 0 ? null
          : (long) (random.nextDouble() * 2 * decimalRange - decimalRange);
    }
    int[] selected = null;
    if (size > 0 && random.nextInt(3) == 0) {
      selected = random.ints(0, size).distinct().limit(1 + random.nextInt(size)).sorted().toArray();
    }
    BytesColumnVector keyColumn = keyColumn(keys);
    LongColumnVector longColumn = longColumn(longs);
    Decimal64ColumnVector decimalColumn = decimal64Column(decimals);
    // A repeating column keeps its first value, which may be NULL.
    for (ColumnVector column : new ColumnVector[] {keyColumn, longColumn, decimalColumn}) {
      if (size > 0 && random.nextInt(6) == 0) {
        repeating(column);
      }
    }
    return aggregationBatch(size, selected, keyColumn, longColumn, decimalColumn);
  }

  private static BytesColumnVector keyColumn(String... keys) {
    BytesColumnVector column = new BytesColumnVector();
    column.initBuffer();
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] == null) {
        column.noNulls = false;
        column.isNull[i] = true;
      } else {
        column.setVal(i, bytes(keys[i]));
      }
    }
    return column;
  }

  private static LongColumnVector longColumn(Long... values) {
    return setLongs(new LongColumnVector(), values);
  }

  private static Decimal64ColumnVector decimal64Column(Long... values) {
    return setLongs(new Decimal64ColumnVector(VectorizedRowBatch.DEFAULT_SIZE, 8, 2), values);
  }

  private static <T extends LongColumnVector> T setLongs(T column, Long[] values) {
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        column.noNulls = false;
        column.isNull[i] = true;
      } else {
        column.vector[i] = values[i];
      }
    }
    return column;
  }

  private static <T extends ColumnVector> T repeating(T column) {
    column.isRepeating = true;
    if (!column.isNull[0]) {
      // The NULLs of the other rows do not count for a repeating non-NULL value.
      column.noNulls = true;
    }
    return column;
  }

  private static VectorizedRowBatch aggregationBatch(int size, int[] selected, ColumnVector... columns) {
    VectorizedRowBatch batch = new VectorizedRowBatch(columns.length);
    System.arraycopy(columns, 0, batch.cols, 0, columns.length);
    setSelection(batch, size, selected);
    return batch;
  }

  /**
   * Runs "select min(l), max(l), sum(l), count(l), min(d), max(d), sum(d), count(d), count(*) from t
   * group by k", l a BIGINT and d a DECIMAL(8,2) read as DECIMAL_64, in hash mode with a single
   * bytes key, once with aggregation columns and once, adding an avg(l) that they do not cover,
   * with aggregation buffers, and checks that both have the same output rows.
   */
  private final class AggregationColumnsGroupBy {
    private final VectorGroupByOperator columns;
    private final VectorGroupByOperator buffers;
    private final List<String> columnsRows = new ArrayList<>();
    private final List<String> buffersRows = new ArrayList<>();
    private int flushes;
    private int compactions;
    private boolean flushOnNextBatch;

    AggregationColumnsGroupBy() throws HiveException {
      columns = build(false, columnsRows);
      buffers = build(true, buffersRows);
    }

    private VectorGroupByOperator build(boolean aggregationBuffers, List<String> rows)
        throws HiveException {
      VectorizationContext ctx = new VectorizationContext("name", Arrays.asList("k", "l", "d"),
          Arrays.asList(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.longTypeInfo, DECIMAL_8_2),
          Arrays.asList(DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE,
              DataTypePhysicalVariation.DECIMAL_64), hconf);
      ArrayList<AggregationDesc> aggregations = new ArrayList<>();
      for (String column : new String[] {"l", "d"}) {
        TypeInfo typeInfo = column.equals("l") ? TypeInfoFactory.longTypeInfo : DECIMAL_8_2;
        for (String aggregate : new String[] {"min", "max", "sum", "count"}) {
          aggregations.add(buildAggregationDesc(ctx, aggregate, GenericUDAFEvaluator.Mode.PARTIAL1,
              column, typeInfo));
        }
      }
      aggregations.add(buildAggregationDescCountStar(ctx));
      if (aggregationBuffers) {
        aggregations.add(buildAggregationDesc(ctx, "avg", GenericUDAFEvaluator.Mode.PARTIAL1, "l",
            TypeInfoFactory.longTypeInfo));
      }
      ArrayList<String> outputColumnNames = new ArrayList<>();
      for (int i = 0; i <= aggregations.size(); i++) {
        outputColumnNames.add("_col" + i);
      }
      GroupByDesc desc = new GroupByDesc();
      desc.setMode(GroupByDesc.Mode.HASH);
      desc.setKeys(new ArrayList<>(Arrays.asList(
          buildColumnDesc(ctx, "k", TypeInfoFactory.stringTypeInfo))));
      desc.setAggregators(aggregations);
      desc.setOutputColumnNames(outputColumnNames);
      desc.setMinReductionHashAggr(0.99f);
      VectorGroupByDesc vectorDesc = new VectorGroupByDesc();
      vectorDesc.setProcessingMode(ProcessingMode.HASH);
      CompilationOpContext cCtx = new CompilationOpContext();
      VectorGroupByOperator vgo = (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(
          OperatorFactory.get(cCtx, desc), ctx, vectorDesc);
      FakeCaptureOutputOperator out = FakeCaptureOutputOperator.addCaptureOutputChild(cCtx, vgo);
      out.setOutputInspector((row, tag) -> addRows((VectorizedRowBatch) row, rows));
      vgo.initialize(hconf, null);
      assertEquals(aggregationBuffers, mode(vgo).aggregationColumns == null);
      return vgo;
    }

    static VectorGroupByOperator.ProcessingModeHashAggregateSingleBytesKey mode(
        VectorGroupByOperator vgo) {
      return (VectorGroupByOperator.ProcessingModeHashAggregateSingleBytesKey) vgo.processingMode;
    }

    /**
     * Adds the key and the first 9 aggregates of each row of the output batch.
     */
    private void addRows(VectorizedRowBatch batch, List<String> rows) {
      BytesColumnVector keys = (BytesColumnVector) batch.cols[0];
      for (int i = 0; i < batch.size; i++) {
        StringBuilder row = new StringBuilder(keys.noNulls || !keys.isNull[i]
            ? new String(keys.vector[i], keys.start[i], keys.length[i], StandardCharsets.ISO_8859_1)
            : "NULL");
        for (int column = 1; column <= 9; column++) {
          LongColumnVector values = (LongColumnVector) batch.cols[column];
          row.append(',').append(values.noNulls || !values.isNull[i]
              ? String.valueOf(values.vector[i]) : "NULL");
        }
        rows.add(row.toString());
      }
    }

    void flushOnNextBatch() {
      flushOnNextBatch = true;
    }

    void process(VectorizedRowBatch batch) throws HiveException {
      VectorGroupByBytesKeyTable table = mode(columns).keyTable;
      int end = table.end();
      int removed = table.end() - table.size();
      for (VectorGroupByOperator vgo : new VectorGroupByOperator[] {columns, buffers}) {
        if (flushOnNextBatch) {
          mode(vgo).gcCanary.clear();
        }
        vgo.process(batch, 0);
      }
      flushOnNextBatch = false;
      if (table.end() < end) {
        flushes++;
        compactions++;
      } else if (table.end() - table.size() > removed) {
        flushes++;
      }
      assertEquals(buffersRows, columnsRows);
    }

    /**
     * Closes both operators and returns the output rows.
     */
    List<String> close() throws HiveException {
      columns.close(false);
      buffers.close(false);
      assertEquals(buffersRows, columnsRows);
      return columnsRows;
    }
  }

  /**
   * Runs "select count(v), sum(v) from t group by k" in hash mode with a single bytes key, keeping
   * the aggregation state in aggregation columns or, with an avg(v) that they do not cover, in
   * aggregation buffers.
   */
  private final class BytesKeyGroupBy {
    private final VectorGroupByOperator vgo;
    private final FakeCaptureVectorToRowOutputOperator out;

    BytesKeyGroupBy(TypeInfo keyTypeInfo) throws HiveException {
      this(keyTypeInfo, false);
    }

    BytesKeyGroupBy(TypeInfo keyTypeInfo, boolean aggregationBuffers) throws HiveException {
      this(keyTypeInfo, aggregationBuffers, desc -> { });
    }

    BytesKeyGroupBy(TypeInfo keyTypeInfo, boolean aggregationBuffers,
        Consumer<GroupByDesc> descSetup) throws HiveException {
      VectorizationContext ctx = new VectorizationContext("name", Arrays.asList("k", "v"));
      Pair<GroupByDesc, VectorGroupByDesc> pair = buildKeyGroupByDesc(ctx, "count", "v",
          TypeInfoFactory.longTypeInfo, new String[] {"k"}, new TypeInfo[] {keyTypeInfo});
      GroupByDesc desc = pair.left;
      desc.getAggregators().add(buildAggregationDesc(ctx, "sum", GenericUDAFEvaluator.Mode.PARTIAL1,
          "v", TypeInfoFactory.longTypeInfo));
      desc.getOutputColumnNames().add("_col2");
      if (aggregationBuffers) {
        desc.getAggregators().add(buildAggregationDesc(ctx, "avg",
            GenericUDAFEvaluator.Mode.PARTIAL1, "v", TypeInfoFactory.longTypeInfo));
        desc.getOutputColumnNames().add("_col3");
      }
      descSetup.accept(desc);
      CompilationOpContext cCtx = new CompilationOpContext();
      vgo = (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(
          OperatorFactory.get(cCtx, desc), ctx, pair.right);
      out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
      vgo.initialize(hconf, null);
      assertTrue(vgo.processingMode
          instanceof VectorGroupByOperator.ProcessingModeHashAggregateSingleBytesKey);
      assertEquals(aggregationBuffers, mode().aggregationColumns == null);
    }

    VectorGroupByOperator.ProcessingModeHashAggregateSingleBytesKey mode() {
      return (VectorGroupByOperator.ProcessingModeHashAggregateSingleBytesKey) vgo.processingMode;
    }

    int accessCount(int entry) {
      return mode().aggregationColumns != null ? mode().aggregationColumns.getAccessCount(entry)
          : mode().keyTable.getRow(entry).getAccessCount();
    }

    void process(VectorizedRowBatch batch) throws HiveException {
      vgo.process(batch, 0);
    }

    /**
     * Closes the operator and returns the count and sum of each key, adding up its output rows.
     */
    Map<String, List<Long>> close() throws HiveException {
      vgo.close(false);
      Map<String, List<Long>> result = new HashMap<>();
      for (Object row : out.getCapturedRows()) {
        Object[] fields = (Object[]) row;
        addCountSum(result, keyString(fields[0]), ((LongWritable) fields[1]).get(),
            ((LongWritable) fields[2]).get());
      }
      return result;
    }
  }

  private final class KeyTableGroupBy {
    private final VectorGroupByOperator vgo;
    private final FakeCaptureVectorToRowOutputOperator out;
    private final int keyCount;

    KeyTableGroupBy(TypeInfo[] keyTypeInfos, boolean aggregationBuffers) throws HiveException {
      this(keyTypeInfos, aggregationBuffers, desc -> { });
    }

    KeyTableGroupBy(TypeInfo[] keyTypeInfos, boolean aggregationBuffers,
        Consumer<GroupByDesc> descSetup) throws HiveException {
      keyCount = keyTypeInfos.length;
      String[] keys = new String[keyCount];
      List<String> columnNames = new ArrayList<>();
      for (int i = 0; i < keyCount; i++) {
        keys[i] = "k" + i;
        columnNames.add(keys[i]);
      }
      columnNames.add("v");
      VectorizationContext ctx = new VectorizationContext("name", columnNames);
      Pair<GroupByDesc, VectorGroupByDesc> pair = buildKeyGroupByDesc(ctx, "count", "v",
          TypeInfoFactory.longTypeInfo, keys, keyTypeInfos);
      GroupByDesc desc = pair.left;
      desc.getAggregators().add(buildAggregationDesc(ctx, "sum", GenericUDAFEvaluator.Mode.PARTIAL1,
          "v", TypeInfoFactory.longTypeInfo));
      desc.getOutputColumnNames().add("_col" + (keyCount + 1));
      if (aggregationBuffers) {
        desc.getAggregators().add(buildAggregationDesc(ctx, "avg",
            GenericUDAFEvaluator.Mode.PARTIAL1, "v", TypeInfoFactory.longTypeInfo));
        desc.getOutputColumnNames().add("_col" + (keyCount + 2));
      }
      descSetup.accept(desc);
      CompilationOpContext cCtx = new CompilationOpContext();
      vgo = (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(
          OperatorFactory.get(cCtx, desc), ctx, pair.right);
      out = FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(cCtx, vgo);
      vgo.initialize(hconf, null);
      assertEquals(keyCount == 1
          ? VectorGroupByOperator.ProcessingModeHashAggregateSingleLongKey.class
          : VectorGroupByOperator.ProcessingModeHashAggregateSerializedKey.class,
          ((Object) vgo.processingMode).getClass());
      assertEquals(aggregationBuffers, mode().aggregationColumns == null);
    }

    VectorGroupByOperator.ProcessingModeHashAggregateKeyTable mode() {
      return (VectorGroupByOperator.ProcessingModeHashAggregateKeyTable) vgo.processingMode;
    }

    void process(VectorizedRowBatch batch) throws HiveException {
      vgo.process(batch, 0);
    }

    /**
     * Closes the operator and returns the count and sum of each key, adding up its output rows.
     */
    Map<List<Object>, List<Long>> close() throws HiveException {
      vgo.close(false);
      Map<List<Object>, List<Long>> result = new HashMap<>();
      for (Object row : out.getCapturedRows()) {
        Object[] fields = (Object[]) row;
        List<Object> key = new ArrayList<>();
        for (int i = 0; i < keyCount; i++) {
          Object field = fields[i];
          key.add(field == null ? null : field instanceof LongWritable
              ? (Object) ((LongWritable) field).get() : field instanceof IntWritable
              ? (Object) (long) ((IntWritable) field).get() : keyString(field));
        }
        addCountSum(result, key, ((LongWritable) fields[keyCount]).get(),
            ((LongWritable) fields[keyCount + 1]).get());
      }
      return result;
    }
  }

  /**
   * Returns the type of each key column: BIGINT for Long values, STRING for String values.
   */
  private static TypeInfo[] keyTypeInfos(Object[][] columns) {
    TypeInfo[] typeInfos = new TypeInfo[columns.length];
    for (int c = 0; c < columns.length; c++) {
      typeInfos[c] = Arrays.stream(columns[c]).anyMatch(v -> v instanceof String)
          ? TypeInfoFactory.stringTypeInfo : TypeInfoFactory.longTypeInfo;
    }
    return typeInfos;
  }

  /**
   * Returns a batch with a key column per array, of Long or String values, a null value being NULL,
   * and value i at row i, and adds its selected rows to expected unless it is null.
   */
  private static VectorizedRowBatch multiKeyBatch(Object[][] columns, int[] selected,
      Map<List<Object>, List<Long>> expected) {
    int size = columns[0].length;
    VectorizedRowBatch batch = new VectorizedRowBatch(columns.length + 1);
    for (int c = 0; c < columns.length; c++) {
      boolean isBytes = Arrays.stream(columns[c]).anyMatch(v -> v instanceof String);
      ColumnVector column = isBytes ? new BytesColumnVector() : new LongColumnVector();
      if (isBytes) {
        ((BytesColumnVector) column).initBuffer();
      }
      for (int i = 0; i < size; i++) {
        Object value = columns[c][i];
        if (value == null) {
          column.noNulls = false;
          column.isNull[i] = true;
        } else if (isBytes) {
          ((BytesColumnVector) column).setVal(i, bytes((String) value));
        } else {
          ((LongColumnVector) column).vector[i] = (Long) value;
        }
      }
      batch.cols[c] = column;
    }
    LongColumnVector valueColumn = new LongColumnVector();
    for (int i = 0; i < size; i++) {
      valueColumn.vector[i] = i;
    }
    batch.cols[columns.length] = valueColumn;
    setSelection(batch, size, selected);
    if (expected != null) {
      addExpected(batch, columns.length, expected);
    }
    return batch;
  }

  /**
   * Adds the count and sum of the selected rows of the batch, read from its columns, to expected.
   */
  private static void addExpected(VectorizedRowBatch batch, int keyCount,
      Map<List<Object>, List<Long>> expected) {
    for (int i = 0; i < batch.size; i++) {
      int row = batch.selectedInUse ? batch.selected[i] : i;
      List<Object> key = new ArrayList<>();
      for (int c = 0; c < keyCount; c++) {
        ColumnVector column = batch.cols[c];
        int r = column.isRepeating ? 0 : row;
        if (!column.noNulls && column.isNull[r]) {
          key.add(null);
        } else if (column instanceof LongColumnVector) {
          key.add(((LongColumnVector) column).vector[r]);
        } else {
          BytesColumnVector bytes = (BytesColumnVector) column;
          key.add(keyString(Arrays.copyOfRange(bytes.vector[r], bytes.start[r],
              bytes.start[r] + bytes.length[r])));
        }
      }
      addCountSum(expected, key, 1, ((LongColumnVector) batch.cols[keyCount]).vector[row]);
    }
  }

  private static <K> void addCountSum(Map<K, List<Long>> result, K key, long count, long sum) {
    result.merge(key, Arrays.asList(count, sum),
        (a, b) -> Arrays.asList(a.get(0) + b.get(0), a.get(1) + b.get(1)));
  }

  private static String keyString(Object key) {
    if (key == null) {
      return null;
    }
    final byte[] bytes;
    if (key instanceof HiveCharWritable) {
      bytes = ((HiveCharWritable) key).getStrippedValue().copyBytes();
    } else if (key instanceof HiveVarcharWritable) {
      bytes = ((HiveVarcharWritable) key).getTextValue().copyBytes();
    } else if (key instanceof BytesWritable) {
      bytes = ((BytesWritable) key).copyBytes();
    } else {
      bytes = ((Text) key).copyBytes();
    }
    return keyString(bytes);
  }

  private static String keyString(byte[] key) {
    return key == null ? null : new String(key, StandardCharsets.ISO_8859_1);
  }

  private static byte[] bytes(String key) {
    return key.getBytes(StandardCharsets.ISO_8859_1);
  }

  private static byte[][] keys(String... keys) {
    return Arrays.stream(keys).map(key -> key == null ? null : bytes(key)).toArray(byte[][]::new);
  }

  /**
   * Returns a batch of the keys, a null key being NULL, with value i at row i, and adds its
   * selected rows to expected. The keys are set by reference to their arrays or copied into the
   * column buffer.
   */
  private static VectorizedRowBatch bytesKeyBatch(byte[][] keys, boolean byRef, int[] selected,
      Map<String, List<Long>> expected) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    BytesColumnVector keyColumn = new BytesColumnVector();
    keyColumn.initBuffer();
    LongColumnVector valueColumn = new LongColumnVector();
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] == null) {
        keyColumn.noNulls = false;
        keyColumn.isNull[i] = true;
      } else if (byRef) {
        keyColumn.setRef(i, keys[i], 0, keys[i].length);
      } else {
        keyColumn.setVal(i, keys[i]);
      }
      valueColumn.vector[i] = i;
    }
    batch.cols[0] = keyColumn;
    batch.cols[1] = valueColumn;
    setSelection(batch, keys.length, selected);
    for (int i = 0; i < batch.size; i++) {
      int row = batch.selectedInUse ? batch.selected[i] : i;
      addCountSum(expected, keyString(keys[row]), 1, row);
    }
    return batch;
  }

  /**
   * Returns a batch of size rows with a repeating key, NULL for a null key, and value i at row i,
   * and adds its selected rows to expected.
   */
  private static VectorizedRowBatch repeatingBytesKeyBatch(byte[] key, int size, int[] selected,
      Map<String, List<Long>> expected) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    BytesColumnVector keyColumn = new BytesColumnVector();
    keyColumn.initBuffer();
    keyColumn.isRepeating = true;
    if (key == null) {
      keyColumn.noNulls = false;
      keyColumn.isNull[0] = true;
    } else {
      keyColumn.setVal(0, key);
    }
    LongColumnVector valueColumn = new LongColumnVector();
    for (int i = 0; i < size; i++) {
      valueColumn.vector[i] = i;
    }
    batch.cols[0] = keyColumn;
    batch.cols[1] = valueColumn;
    setSelection(batch, size, selected);
    for (int i = 0; i < batch.size; i++) {
      addCountSum(expected, keyString(key), 1, batch.selectedInUse ? batch.selected[i] : i);
    }
    return batch;
  }

  private static void setSelection(VectorizedRowBatch batch, int size, int[] selected) {
    if (selected == null) {
      batch.size = size;
    } else {
      batch.selectedInUse = true;
      System.arraycopy(selected, 0, batch.selected, 0, selected.length);
      batch.size = selected.length;
    }
  }
}
