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

package org.apache.hive.benchmark.vectorization.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeCaptureOutputOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Hash GROUP BY through the real {@link VectorGroupByOperator} in HASH mode (PARTIAL1) on the key
 * shapes of the TPC-DS GROUP BY clauses: {@code shape} is one letter per key column, L for BIGINT
 * and S for STRING. min, max, sum and count over a DECIMAL_64 decimal(5,2) column. Scores are ns
 * per input row; rows are generated during the run, so subtract {@link #generatorOnly}.
 * <p>
 * Every row draws a group uniformly from {@code groups}. A key of two or more columns splits the
 * group into a first column of groups / 50 values and a second of 50 (4 below 20000 groups); further
 * columns depend on the first, as the customer attributes of TPC-DS q4 depend on the customer.
 * BIGINT values are dense from {@code longBase}, as surrogate keys are; STRING values are 8..20 bytes
 * in the first column and 6..13 in the others, ending with their number in 5 base-62 digits.
 * {@code memory} 0.05 forces partial flushes at 5e6 groups.
 * <p>
 * One untimed run at setup is checked in full against a plain-Java aggregation, merging the partial
 * rows of every flush per group; every timed run is checked by its total count and sum.
 * <pre>
 * java -jar hive-jmh/target/benchmarks.jar 'VectorGroupByMultiKeyBench.(groupBy|generatorOnly)' \
 *     -p shape=LS -p groups=20000
 * </pre>
 */
@Fork(value = 1, jvmArgsAppend = {"-Xms4g", "-Xmx4g", "-XX:+UseG1GC",
    "--add-opens=java.base/java.net=ALL-UNNAMED", "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED", "--add-opens=java.base/java.util.regex=ALL-UNNAMED",
    "--add-opens=java.sql/java.sql=ALL-UNNAMED"})
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class VectorGroupByMultiKeyBench {

  static final int ROWS = 48_829 * 1024;
  private static final int BATCH = VectorizedRowBatch.DEFAULT_SIZE;
  private static final TypeInfo DEC52 = TypeInfoFactory.getDecimalTypeInfo(5, 2);
  private static final String DIGITS = VectorGroupByBytesKeyBench.HighCardinality.DIGITS;

  @State(Scope.Benchmark)
  public static class Keys {
    @Param({"L", "LL", "LS", "SS", "S", "LLL", "SLSSSS"})
    public String shape;

    @Param({"412", "20000", "1000000", "5000000"})
    public int groups;

    @Param({"0.5"})
    public float memory;

    // The smallest BIGINT value; 4294967296 makes every value take more than 32 bits.
    @Param({"1"})
    public long longBase;

    final HiveConf conf = new HiveConf();
    int columns;
    int split;
    // Per key column: STRING values in one pool, or none for BIGINT.
    byte[][] pools;
    int[][] offsets;
    byte[][] lengths;
    VectorizedRowBatch vrb;
    long expectedSum;
    long outRows;
    long countSum;
    long sumSum;

    @Setup(Level.Trial)
    public void setup() throws HiveException {
      columns = shape.length();
      split = columns == 1 ? 1 : groups >= 20000 ? 50 : 4;
      pools = new byte[columns][];
      offsets = new int[columns][];
      lengths = new byte[columns][];
      vrb = new VectorizedRowBatch(columns + 1, BATCH);
      SplittableRandom random = new SplittableRandom(42);
      for (int p = 0; p < columns; p++) {
        if (shape.charAt(p) == 'S') {
          buildPool(p, domain(p), p == 0 ? 8 : 6, p == 0 ? 13 : 8, random);
          vrb.cols[p] = new BytesColumnVector(BATCH);
        } else {
          vrb.cols[p] = new LongColumnVector(BATCH);
        }
      }
      vrb.cols[columns] = new Decimal64ColumnVector(BATCH, 5, 2);
      verify();
    }

    int domain(int column) {
      return column == 1 ? split : groups / split;
    }

    int component(int group, int column) {
      return column == 1 ? group % split : group / split;
    }

    private void buildPool(int p, int n, int minLength, int lengthRange, SplittableRandom random) {
      offsets[p] = new int[n];
      lengths[p] = new byte[n];
      int[] lens = new int[n];
      long total = 0;
      for (int k = 0; k < n; k++) {
        lens[k] = minLength + random.nextInt(lengthRange);
        total += lens[k];
      }
      byte[] pool = pools[p] = new byte[(int) total];
      int off = 0;
      for (int k = 0; k < n; k++) {
        int len = lens[k];
        offsets[p][k] = off;
        lengths[p][k] = (byte) len;
        for (int j = 0; j < len - 5; j++) {
          pool[off + j] = (byte) ('a' + random.nextInt(26));
        }
        int v = k;
        for (int j = len - 1; j >= len - 5; j--) {
          pool[off + j] = (byte) DIGITS.charAt(v % 62);
          v /= 62;
        }
        off += len;
      }
    }

    /** Rows of one run; every run draws the same rows. */
    final class Generator {
      final SplittableRandom random = new SplittableRandom(7);
      final int[] group = new int[BATCH];

      void fill(VectorizedRowBatch batch) {
        long[] values = ((Decimal64ColumnVector) batch.cols[columns]).vector;
        for (int i = 0; i < BATCH; i++) {
          long r = random.nextLong();
          group[i] = (int) ((r >>> 20) % groups);
          values[i] = (int) (r >>> 44) % 19999 - 9999;
        }
        for (int p = 0; p < columns; p++) {
          if (pools[p] == null) {
            long[] v = ((LongColumnVector) batch.cols[p]).vector;
            // A further column depends on the first one, with other values.
            long scale = p > 1 ? 31 : 1;
            for (int i = 0; i < BATCH; i++) {
              v[i] = component(group[i], p) * scale + longBase;
            }
          } else {
            BytesColumnVector key = (BytesColumnVector) batch.cols[p];
            byte[] pool = pools[p];
            int[] offs = offsets[p];
            byte[] lens = lengths[p];
            for (int i = 0; i < BATCH; i++) {
              int c = component(group[i], p);
              key.vector[i] = pool;
              key.start[i] = offs[c];
              key.length[i] = lens[c];
            }
          }
        }
        batch.size = BATCH;
      }
    }

    int decode(VectorizedRowBatch b, int p, int i) {
      ColumnVector c = b.cols[p];
      if (!c.noNulls && c.isNull[c.isRepeating ? 0 : i]) {
        throw new IllegalStateException("unexpected NULL key");
      }
      if (c instanceof LongColumnVector l) {
        return (int) (l.vector[l.isRepeating ? 0 : i] - longBase);
      }
      BytesColumnVector k = (BytesColumnVector) c;
      int row = k.isRepeating ? 0 : i;
      int end = k.start[row] + k.length[row];
      int id = 0;
      for (int j = end - 5; j < end; j++) {
        id = id * 62 + DIGITS.indexOf(k.vector[row][j]);
      }
      return id;
    }

    int group(VectorizedRowBatch b, int i) {
      return columns == 1 ? decode(b, 0, i) : decode(b, 0, i) * split + decode(b, 1, i);
    }

    VectorGroupByOperator newOperator(FakeCaptureOutputOperator.OutputInspector inspector)
        throws HiveException {
      CompilationOpContext ctx = new CompilationOpContext();
      VectorGroupByOperator op = operator(ctx, conf, shape, memory);
      FakeCaptureOutputOperator out = FakeCaptureOutputOperator.addCaptureOutputChild(ctx, op);
      op.initialize(conf, null);
      out.setOutputInspector(inspector);
      return op;
    }

    /** One untimed run whose partial rows are merged per group and compared with the generated rows. */
    private void verify() throws HiveException {
      int[] count = new int[groups];
      long[] sum = new long[groups];
      short[] min = new short[groups];
      short[] max = new short[groups];
      short[] outMin = new short[groups];
      short[] outMax = new short[groups];
      Arrays.fill(min, Short.MAX_VALUE);
      Arrays.fill(max, Short.MIN_VALUE);
      Arrays.fill(outMin, Short.MAX_VALUE);
      Arrays.fill(outMax, Short.MIN_VALUE);
      long[] partialRows = new long[1];
      final int n = columns;
      VectorGroupByOperator op = newOperator((row, tag) -> {
        VectorizedRowBatch b = (VectorizedRowBatch) row;
        for (int i = 0; i < b.size; i++) {
          int g = group(b, i);
          count[g] -= (int) VectorGroupByBytesKeyBench.longAt(b.cols[n + 3], i, 0);
          sum[g] -= VectorGroupByBytesKeyBench.longAt(b.cols[n + 2], i, 2);
          outMin[g] = (short) Math.min(outMin[g], VectorGroupByBytesKeyBench.longAt(b.cols[n], i, 2));
          outMax[g] = (short) Math.max(outMax[g], VectorGroupByBytesKeyBench.longAt(b.cols[n + 1], i, 2));
        }
        partialRows[0] += b.size;
      });
      String mode = VectorGroupByBytesKeyBench.describe(op);
      Generator gen = new Generator();
      expectedSum = 0;
      for (long r = 0; r < ROWS; r += BATCH) {
        gen.fill(vrb);
        long[] values = ((Decimal64ColumnVector) vrb.cols[n]).vector;
        for (int i = 0; i < BATCH; i++) {
          int g = gen.group[i];
          count[g]++;
          sum[g] += values[i];
          min[g] = (short) Math.min(min[g], values[i]);
          max[g] = (short) Math.max(max[g], values[i]);
          expectedSum += values[i];
        }
        op.process(vrb, 0);
      }
      op.close(false);
      int seen = 0;
      for (int g = 0; g < groups; g++) {
        if (count[g] != 0 || sum[g] != 0 || min[g] != outMin[g] || max[g] != outMax[g]) {
          throw new IllegalStateException("group " + g + " differs from the reference");
        }
        if (min[g] != Short.MAX_VALUE) {
          seen++;
        }
      }
      System.out.printf("%nMULTIKEY shape=%s groups=%d memory=%.2f %s seen=%d partialRows=%d"
          + " verified=true%n", shape, groups, memory, mode, seen, partialRows[0]);
    }

    FakeCaptureOutputOperator.OutputInspector countingInspector() {
      outRows = 0;
      countSum = 0;
      sumSum = 0;
      final int n = columns;
      return (row, tag) -> {
        VectorizedRowBatch b = (VectorizedRowBatch) row;
        outRows += b.size;
        for (int i = 0; i < b.size; i++) {
          countSum += VectorGroupByBytesKeyBench.longAt(b.cols[n + 3], i, 0);
          sumSum += VectorGroupByBytesKeyBench.longAt(b.cols[n + 2], i, 2);
        }
      };
    }

    @TearDown(Level.Iteration)
    public void check() {
      if (outRows != 0 && (countSum != ROWS || sumSum != expectedSum)) {
        throw new IllegalStateException("count " + countSum + " sum " + sumSum + " vs " + ROWS + " "
            + expectedSum);
      }
    }
  }

  @Benchmark
  @OperationsPerInvocation(ROWS)
  public void groupBy(Keys s) throws HiveException {
    VectorGroupByOperator op = s.newOperator(s.countingInspector());
    Keys.Generator gen = s.new Generator();
    for (long n = 0; n < ROWS; n += BATCH) {
      gen.fill(s.vrb);
      op.process(s.vrb, 0);
    }
    op.close(false);
  }

  /** The row generation that {@link #groupBy} includes, to subtract from it. */
  @Benchmark
  @OperationsPerInvocation(ROWS)
  public long generatorOnly(Keys s) {
    Keys.Generator gen = s.new Generator();
    long check = 0;
    for (long n = 0; n < ROWS; n += BATCH) {
      gen.fill(s.vrb);
      check += gen.group[5];
    }
    return check;
  }

  static VectorGroupByOperator operator(CompilationOpContext ctx, HiveConf conf, String shape,
      float memoryUsage) throws HiveException {
    List<String> names = new ArrayList<>();
    List<TypeInfo> types = new ArrayList<>();
    List<DataTypePhysicalVariation> variations = new ArrayList<>();
    ArrayList<ExprNodeDesc> keys = new ArrayList<>();
    for (int p = 0; p < shape.length(); p++) {
      TypeInfo type = shape.charAt(p) == 'S' ? TypeInfoFactory.stringTypeInfo : TypeInfoFactory.longTypeInfo;
      names.add("k" + p);
      types.add(type);
      variations.add(DataTypePhysicalVariation.NONE);
      keys.add(new ExprNodeColumnDesc(type, "k" + p, "t", false));
    }
    names.add("measure");
    types.add(DEC52);
    variations.add(DataTypePhysicalVariation.DECIMAL_64);
    VectorizationContext vctx = new VectorizationContext("bench", names, types, variations, conf);
    ArrayList<AggregationDesc> aggs = new ArrayList<>();
    aggs.add(agg("min", new GenericUDAFMin.GenericUDAFMinEvaluator()));
    aggs.add(agg("max", new GenericUDAFMax.GenericUDAFMaxEvaluator()));
    try {
      aggs.add(agg("sum", new GenericUDAFSum().getEvaluator(new TypeInfo[] {DEC52})));
    } catch (org.apache.hadoop.hive.ql.parse.SemanticException e) {
      throw new HiveException(e);
    }
    aggs.add(agg("count", new GenericUDAFCount.GenericUDAFCountEvaluator()));
    ArrayList<String> outputs = new ArrayList<>();
    for (int i = 0; i < shape.length() + aggs.size(); i++) {
      outputs.add("_col" + i);
    }
    GroupByDesc desc = new GroupByDesc();
    desc.setMode(GroupByDesc.Mode.HASH);
    desc.setKeys(keys);
    desc.setAggregators(aggs);
    desc.setOutputColumnNames(outputs);
    desc.setGroupByMemoryUsage(memoryUsage);
    desc.setMinReductionHashAggr(0.99f);
    VectorGroupByDesc vdesc = new VectorGroupByDesc();
    vdesc.setProcessingMode(VectorGroupByDesc.ProcessingMode.HASH);
    Operator<? extends OperatorDesc> gby = OperatorFactory.get(ctx, desc);
    return (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(gby, vctx, vdesc);
  }

  private static AggregationDesc agg(String name, GenericUDAFEvaluator eval) {
    ArrayList<ExprNodeDesc> params = new ArrayList<>();
    params.add(new ExprNodeColumnDesc(DEC52, "measure", "t", false));
    AggregationDesc a = new AggregationDesc();
    a.setGenericUDAFName(name);
    a.setMode(GenericUDAFEvaluator.Mode.PARTIAL1);
    a.setParameters(params);
    a.setGenericUDAFEvaluator(eval);
    return a;
  }
}
