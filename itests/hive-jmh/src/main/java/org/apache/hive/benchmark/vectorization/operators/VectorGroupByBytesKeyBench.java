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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeCaptureOutputOperator;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeCaptureVectorToRowOutputOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
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
 * Hash GROUP BY on a single STRING key through the real {@link VectorGroupByOperator} in HASH mode
 * (PARTIAL1): min, max, sum and count over a DECIMAL_64 decimal(5,2) column. That is the map side of
 * {@code SELECT station, min(measure), max(measure), avg(measure) ... GROUP BY station} once CBO has
 * turned avg into sum and count (1TRC part 1). Scores are ns per input row: one invocation is one
 * operator over a fixed number of rows, closed at the end so the final flush is timed.
 * <p>
 * Every cell checks the operator's output against a plain-Java aggregation of the same rows and fails
 * on a mismatch. {@link #part1} checks every iteration in full. {@link #highCardinality} checks one
 * untimed run in full at setup, merging the partial rows of every flush per key, and every timed run
 * by its total count and sum.
 * <p>
 * Each fork prints the processing mode the operator chose and whether it keeps aggregation columns, so
 * a result states the path it measured.
 * <ul>
 *   <li>{@link #part1}: 412 keys of 3..13 bytes (mean ~7.6), as a Parquet dictionary decoder hands them
 *   over: every row references one flattened per-column-chunk dictionary, which changes every 2^20
 *   rows. {@code order}: random, cyclic (key = row mod 412, all 412 keys in every batch) or clustered
 *   (runs of 64 equal keys, 16 keys per batch). {@code aggregates=withAvg} adds avg, which the
 *   aggregation columns do not cover.</li>
 *   <li>{@link #highCardinality}: 8..20-byte keys drawn uniformly from {@code distinct_1e6} or
 *   {@code distinct_5e6} keys (with {@code _lowmem}: hive.map.aggr.hash.percentmemory 0.05, which
 *   forces partial flushes); {@code growth}: the key range grows geometrically from 1e3 to 1e7 over
 *   the run; {@code hot90_20m}: 90% of the rows on 412 hot keys, the rest over 20M keys. Rows are
 *   generated during the run, so subtract {@link #generatorOnly} for the same scenario.</li>
 * </ul>
 * How to run:
 * <pre>
 * mvn clean install -DskipTests                                   # from the root
 * cd itests &amp;&amp; mvn clean install -DskipTests -Pperf -pl hive-jmh -am
 * java -jar hive-jmh/target/benchmarks.jar VectorGroupByBytesKeyBench.part1 -p order=random
 * java -jar hive-jmh/target/benchmarks.jar 'VectorGroupByBytesKeyBench.(highCardinality|generatorOnly)' \
 *     -p scenario=distinct_5e6
 * </pre>
 * Run on a quiet machine. Fork JVM flags are fixed below; a GROUP BY variant is compared by putting
 * its classes in front of the jar: {@code java -cp variant-classes:benchmarks.jar org.openjdk.jmh.Main ...}
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
public class VectorGroupByBytesKeyBench {

  // Exact multiples of the 1024-row batch: every benchmark loop is `for (n = 0; n < ROWS; n += BATCH)`,
  // which always processes a whole number of batches, so a non-multiple would silently process more
  // rows than it names (measured: 50_000_000 ran 50_000_896).
  static final int PART1_ROWS = 195_313 * 1024;   // ~200M
  static final int HIGH_ROWS = 48_829 * 1024;     // ~50M
  private static final int BATCH = VectorizedRowBatch.DEFAULT_SIZE;
  private static final TypeInfo DEC52 = TypeInfoFactory.getDecimalTypeInfo(5, 2);

  @State(Scope.Benchmark)
  public static class Part1 {
    @Param({"random", "cyclic", "clustered"})
    public String order;

    @Param({"false", "true"})
    public boolean selectedInUse;

    @Param({"minMaxSumCount", "withAvg"})
    public String aggregates;

    static final int NDV = 412;
    static final int NDICT = 8;
    static final int BATCHES_PER_DICT = 32;
    static final long ROWS_PER_DICT = 1L << 20;
    // City-name lengths 3..13, 1BRC-like: mode 6-8 and a long tail.
    static final int[] LENGTH_WEIGHTS = {4, 22, 48, 68, 72, 60, 44, 32, 24, 18, 20};

    final HiveConf conf = new HiveConf();
    VectorizedRowBatch[][] batches;
    Map<String, String> expected;
    final Map<String, String> result = new TreeMap<>();
    boolean duplicate;
    int dict;
    int batch;
    long inDict;

    @Setup(Level.Trial)
    public void setup() throws HiveException {
      byte[][] names = names();
      int[][] perm = new int[NDICT][];
      int[][][] ids = new int[NDICT][BATCHES_PER_DICT][BATCH];
      batches = new VectorizedRowBatch[NDICT][BATCHES_PER_DICT];
      Random permRandom = new Random(3);
      Random keyRandom = new Random(11);
      Random valueRandom = new Random(7);
      for (int d = 0; d < NDICT; d++) {
        perm[d] = new int[NDV];
        for (int i = 0; i < NDV; i++) {
          perm[d][i] = i;
        }
        for (int i = NDV - 1; i > 0; i--) {
          int j = permRandom.nextInt(i + 1);
          int t = perm[d][i];
          perm[d][i] = perm[d][j];
          perm[d][j] = t;
        }
        int[] offsets = new int[NDV + 1];
        for (int i = 0; i < NDV; i++) {
          offsets[i + 1] = offsets[i] + names[perm[d][i]].length;
        }
        byte[] dictionary = new byte[offsets[NDV]];
        for (int i = 0; i < NDV; i++) {
          System.arraycopy(names[perm[d][i]], 0, dictionary, offsets[i], names[perm[d][i]].length);
        }
        for (int b = 0; b < BATCHES_PER_DICT; b++) {
          VectorizedRowBatch vrb = new VectorizedRowBatch(2, BATCH);
          BytesColumnVector key = new BytesColumnVector(BATCH);
          Decimal64ColumnVector value = new Decimal64ColumnVector(BATCH, 5, 2);
          for (int i = 0; i < BATCH; i++) {
            int row = b * BATCH + i;
            int id = switch (order) {
              case "random" -> keyRandom.nextInt(NDV);
              case "cyclic" -> row % NDV;
              case "clustered" -> ((d * BATCHES_PER_DICT * BATCH + row) / 64) % NDV;
              default -> throw new IllegalArgumentException(order);
            };
            ids[d][b][i] = id;
            key.setRef(i, dictionary, offsets[id], offsets[id + 1] - offsets[id]);
            value.vector[i] = valueRandom.nextInt(19999) - 9999;
          }
          vrb.cols[0] = key;
          vrb.cols[1] = value;
          vrb.size = BATCH;
          vrb.selectedInUse = selectedInUse;
          if (selectedInUse) {
            for (int i = 0; i < BATCH; i++) {
              vrb.selected[i] = i;
            }
          }
          batches[d][b] = vrb;
        }
      }
      // The expected output, walking the same stream.
      long[][] acc = new long[NDV][];
      rewind();
      for (long n = 0; n < PART1_ROWS; n += BATCH) {
        int d = dict;
        int b = batch;
        long[] v = ((Decimal64ColumnVector) next().cols[1]).vector;
        for (int i = 0; i < BATCH; i++) {
          int station = perm[d][ids[d][b][i]];
          long[] a = acc[station];
          if (a == null) {
            a = acc[station] = new long[] {Long.MAX_VALUE, Long.MIN_VALUE, 0, 0};
          }
          a[0] = Math.min(a[0], v[i]);
          a[1] = Math.max(a[1], v[i]);
          a[2] += v[i];
          a[3]++;
        }
      }
      expected = new TreeMap<>();
      for (int k = 0; k < NDV; k++) {
        long[] a = acc[k];
        if (a != null) {
          expected.put(new String(names[k], java.nio.charset.StandardCharsets.UTF_8),
              dec(a[0]) + "," + dec(a[1]) + "," + dec(a[2]) + "," + a[3]);
        }
      }
      VectorGroupByOperator op = newOperator();
      System.out.printf("%nPART1 order=%s selectedInUse=%s aggregates=%s groups=%d %s expectedChecksum=%016x%n",
          order, selectedInUse, aggregates, expected.size(), describe(op), checksum(expected));
      op.close(true);
    }

    private static byte[][] names() {
      Random random = new Random(42);
      int total = 0;
      for (int w : LENGTH_WEIGHTS) {
        total += w;
      }
      byte[][] names = new byte[NDV][];
      HashSet<String> seen = new HashSet<>();
      for (int k = 0; k < NDV; k++) {
        String s;
        do {
          int pick = random.nextInt(total);
          int len = 3;
          for (int i = 0; i < LENGTH_WEIGHTS.length; i++) {
            pick -= LENGTH_WEIGHTS[i];
            if (pick < 0) {
              len = 3 + i;
              break;
            }
          }
          StringBuilder sb = new StringBuilder();
          sb.append((char) ('A' + random.nextInt(26)));
          for (int j = 1; j < len; j++) {
            sb.append((char) ('a' + random.nextInt(26)));
          }
          s = sb.toString();
        } while (!seen.add(s));
        names[k] = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      }
      return names;
    }

    void rewind() {
      dict = 0;
      batch = 0;
      inDict = 0;
    }

    /** The next batch: dictionary chunk d for ROWS_PER_DICT rows, cycling its batches, then d + 1. */
    VectorizedRowBatch next() {
      VectorizedRowBatch vrb = batches[dict][batch];
      if (++batch == BATCHES_PER_DICT) {
        batch = 0;
      }
      inDict += BATCH;
      if (inDict >= ROWS_PER_DICT) {
        inDict = 0;
        batch = 0;
        dict = (dict + 1) % NDICT;
      }
      return vrb;
    }

    VectorGroupByOperator newOperator() throws HiveException {
      result.clear();
      duplicate = false;
      boolean withAvg = "withAvg".equals(aggregates);
      CompilationOpContext ctx = new CompilationOpContext();
      VectorGroupByOperator op = operator(ctx, conf, withAvg, 0.5f);
      FakeCaptureVectorToRowOutputOperator out =
          FakeCaptureVectorToRowOutputOperator.addCaptureOutputChild(ctx, op);
      op.initialize(conf, null);
      out.setOutputInspector((row, tag) -> {
        Object[] r = (Object[]) row;
        if (result.put(String.valueOf(r[0]), r[1] + "," + r[2] + "," + r[3] + "," + r[4]) != null) {
          duplicate = true;
        }
      });
      return op;
    }

    @TearDown(Level.Iteration)
    public void check() {
      if (duplicate || !result.equals(expected)) {
        throw new IllegalStateException("output differs from the reference: " + result.size() + " groups vs "
            + expected.size() + (duplicate ? ", duplicate group" : "") + ", checksum "
            + Long.toHexString(checksum(result)) + " vs " + Long.toHexString(checksum(expected)));
      }
    }
  }

  @State(Scope.Benchmark)
  public static class HighCardinality {
    @Param({"distinct_1e6", "distinct_5e6", "distinct_5e6_lowmem", "growth", "hot90_20m"})
    public String scenario;

    static final int HOT = 412;
    static final String DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    final HiveConf conf = new HiveConf();
    int ndv;
    double hotFraction;
    boolean growth;
    float memoryUsage = 0.5f;
    // Key pool: ndv keys, then HOT hot keys, in one array. Each key ends with its number in 5 base-62
    // digits, so an output key gives back its number.
    byte[] pool;
    int[] offsets;
    byte[] lengths;
    final VectorizedRowBatch vrb = newBatch();
    long expectedSum;
    long outRows;
    long countSum;
    long sumSum;

    @Setup(Level.Trial)
    public void setup() throws HiveException {
      switch (scenario) {
      case "distinct_1e6" -> ndv = 1_000_000;
      case "distinct_5e6" -> ndv = 5_000_000;
      case "distinct_5e6_lowmem" -> {
        ndv = 5_000_000;
        memoryUsage = 0.05f;
      }
      case "growth" -> {
        ndv = 10_000_000;
        growth = true;
      }
      case "hot90_20m" -> {
        ndv = 20_000_000;
        hotFraction = 0.9;
      }
      default -> throw new IllegalArgumentException(scenario);
      }
      buildPool();
      verify();
    }

    private void buildPool() {
      int n = ndv + HOT;
      SplittableRandom random = new SplittableRandom(42);
      offsets = new int[n];
      lengths = new byte[n];
      int[] lens = new int[n];
      long total = 0;
      for (int k = 0; k < n; k++) {
        lens[k] = 8 + random.nextInt(13);
        total += lens[k];
      }
      pool = new byte[(int) total];
      int off = 0;
      for (int k = 0; k < n; k++) {
        int len = lens[k];
        offsets[k] = off;
        lengths[k] = (byte) len;
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

    static VectorizedRowBatch newBatch() {
      VectorizedRowBatch batch = new VectorizedRowBatch(2, BATCH);
      batch.cols[0] = new BytesColumnVector(BATCH);
      batch.cols[1] = new Decimal64ColumnVector(BATCH, 5, 2);
      return batch;
    }

    /** Rows of one run; every run draws the same rows. */
    final class Generator {
      final SplittableRandom random = new SplittableRandom(7);
      final int hotThreshold = (int) (hotFraction * (1 << 20));
      long rows;

      void fill(VectorizedRowBatch batch) {
        BytesColumnVector key = (BytesColumnVector) batch.cols[0];
        long[] values = ((Decimal64ColumnVector) batch.cols[1]).vector;
        int range = growth ? (int) Math.min(ndv, 1e3 * Math.pow(1e4, rows / (double) HIGH_ROWS)) : ndv;
        for (int i = 0; i < BATCH; i++) {
          long r = random.nextLong();
          int k = ((int) r & 0xFFFFF) < hotThreshold
              ? ndv + (int) ((r >>> 20) % HOT)
              : (int) ((r >>> 20) % range);
          key.vector[i] = pool;
          key.start[i] = offsets[k];
          key.length[i] = lengths[k];
          values[i] = (int) (r >>> 44) % 19999 - 9999;
        }
        batch.size = BATCH;
        rows += BATCH;
      }
    }

    VectorGroupByOperator newOperator(FakeCaptureOutputOperator.OutputInspector inspector)
        throws HiveException {
      CompilationOpContext ctx = new CompilationOpContext();
      VectorGroupByOperator op = operator(ctx, conf, false, memoryUsage);
      FakeCaptureOutputOperator out = FakeCaptureOutputOperator.addCaptureOutputChild(ctx, op);
      op.initialize(conf, null);
      out.setOutputInspector(inspector);
      return op;
    }

    /** One untimed run whose partial rows are merged per key and compared with the generated rows. */
    private void verify() throws HiveException {
      int n = ndv + HOT;
      int[] count = new int[n];
      long[] sum = new long[n];
      short[] min = new short[n];
      short[] max = new short[n];
      short[] outMin = new short[n];
      short[] outMax = new short[n];
      java.util.Arrays.fill(min, Short.MAX_VALUE);
      java.util.Arrays.fill(max, Short.MIN_VALUE);
      java.util.Arrays.fill(outMin, Short.MAX_VALUE);
      java.util.Arrays.fill(outMax, Short.MIN_VALUE);
      int[] digit = new int[128];
      for (int i = 0; i < DIGITS.length(); i++) {
        digit[DIGITS.charAt(i)] = i;
      }
      long[] partialRows = new long[1];
      VectorGroupByOperator op = newOperator((row, tag) -> {
        VectorizedRowBatch b = (VectorizedRowBatch) row;
        BytesColumnVector k = (BytesColumnVector) b.cols[0];
        for (int i = 0; i < b.size; i++) {
          int end = k.start[i] + k.length[i];
          int id = 0;
          for (int j = end - 5; j < end; j++) {
            id = id * 62 + digit[k.vector[i][j]];
          }
          count[id] -= (int) longAt(b.cols[4], i, 0);
          sum[id] -= longAt(b.cols[3], i, 2);
          outMin[id] = (short) Math.min(outMin[id], longAt(b.cols[1], i, 2));
          outMax[id] = (short) Math.max(outMax[id], longAt(b.cols[2], i, 2));
        }
        partialRows[0] += b.size;
      });
      String mode = describe(op);
      Generator gen = new Generator();
      for (long r = 0; r < HIGH_ROWS; r += BATCH) {
        gen.fill(vrb);
        BytesColumnVector key = (BytesColumnVector) vrb.cols[0];
        long[] values = ((Decimal64ColumnVector) vrb.cols[1]).vector;
        for (int i = 0; i < BATCH; i++) {
          int end = key.start[i] + key.length[i];
          int id = 0;
          for (int j = end - 5; j < end; j++) {
            id = id * 62 + digit[pool[j]];
          }
          count[id]++;
          sum[id] += values[i];
          min[id] = (short) Math.min(min[id], values[i]);
          max[id] = (short) Math.max(max[id], values[i]);
          expectedSum += values[i];
        }
        op.process(vrb, 0);
      }
      op.close(false);
      int groups = 0;
      for (int id = 0; id < n; id++) {
        if (count[id] != 0 || sum[id] != 0 || min[id] != outMin[id] || max[id] != outMax[id]) {
          throw new IllegalStateException("key " + id + " differs from the reference");
        }
        if (min[id] != Short.MAX_VALUE) {
          groups++;
        }
      }
      System.out.printf("%nHIGH scenario=%s ndv=%d rows=%d memoryUsage=%.2f %s groups=%d partialRows=%d"
          + " verified=true%n", scenario, ndv, HIGH_ROWS, memoryUsage, mode, groups, partialRows[0]);
    }

    FakeCaptureOutputOperator.OutputInspector countingInspector() {
      outRows = 0;
      countSum = 0;
      sumSum = 0;
      return (row, tag) -> {
        VectorizedRowBatch b = (VectorizedRowBatch) row;
        outRows += b.size;
        for (int i = 0; i < b.size; i++) {
          countSum += longAt(b.cols[4], i, 0);
          sumSum += longAt(b.cols[3], i, 2);
        }
      };
    }

    @TearDown(Level.Iteration)
    public void check() {
      if (outRows != 0 && (countSum != HIGH_ROWS || sumSum != expectedSum)) {
        throw new IllegalStateException("count " + countSum + " sum " + sumSum + " vs " + HIGH_ROWS + " "
            + expectedSum);
      }
    }
  }

  @Benchmark
  @OperationsPerInvocation(PART1_ROWS)
  public void part1(Part1 s) throws HiveException {
    VectorGroupByOperator op = s.newOperator();
    s.rewind();
    for (long n = 0; n < PART1_ROWS; n += BATCH) {
      op.process(s.next(), 0);
    }
    op.close(false);
  }

  @Benchmark
  @OperationsPerInvocation(HIGH_ROWS)
  public void highCardinality(HighCardinality s) throws HiveException {
    VectorGroupByOperator op = s.newOperator(s.countingInspector());
    HighCardinality.Generator gen = s.new Generator();
    for (long n = 0; n < HIGH_ROWS; n += BATCH) {
      gen.fill(s.vrb);
      op.process(s.vrb, 0);
    }
    op.close(false);
  }

  /** The row generation that {@link #highCardinality} includes, to subtract from it. */
  @Benchmark
  @OperationsPerInvocation(HIGH_ROWS)
  public long generatorOnly(HighCardinality s) {
    HighCardinality.Generator gen = s.new Generator();
    long check = 0;
    for (long n = 0; n < HIGH_ROWS; n += BATCH) {
      gen.fill(s.vrb);
      check += ((BytesColumnVector) s.vrb.cols[0]).start[5];
    }
    return check;
  }

  static VectorGroupByOperator operator(CompilationOpContext ctx, HiveConf conf, boolean withAvg,
      float memoryUsage) throws HiveException {
    VectorizationContext vctx = new VectorizationContext("bench", List.of("station", "measure"),
        List.of(TypeInfoFactory.stringTypeInfo, DEC52),
        List.of(DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.DECIMAL_64), conf);
    ArrayList<AggregationDesc> aggs = new ArrayList<>();
    aggs.add(agg("min", new GenericUDAFMin.GenericUDAFMinEvaluator()));
    aggs.add(agg("max", new GenericUDAFMax.GenericUDAFMaxEvaluator()));
    try {
      aggs.add(agg("sum", new GenericUDAFSum().getEvaluator(new TypeInfo[] {DEC52})));
      aggs.add(agg("count", new GenericUDAFCount.GenericUDAFCountEvaluator()));
      if (withAvg) {
        aggs.add(agg("avg", new GenericUDAFAverage().getEvaluator(new TypeInfo[] {DEC52})));
      }
    } catch (org.apache.hadoop.hive.ql.parse.SemanticException e) {
      throw new HiveException(e);
    }
    ArrayList<String> outputs = new ArrayList<>();
    for (int i = 0; i <= aggs.size(); i++) {
      outputs.add("_col" + i);
    }
    GroupByDesc desc = new GroupByDesc();
    desc.setMode(GroupByDesc.Mode.HASH);
    ArrayList<ExprNodeDesc> keys = new ArrayList<>();
    keys.add(new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "station", "t", false));
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

  /** The processing mode of the operator, and whether it keeps aggregation columns. */
  static String describe(VectorGroupByOperator op) {
    try {
      Field f = VectorGroupByOperator.class.getDeclaredField("processingMode");
      f.setAccessible(true);
      Object mode = f.get(op);
      String columns = "n/a";
      for (Class<?> c = mode.getClass(); c != null; c = c.getSuperclass()) {
        try {
          Field columnsField = c.getDeclaredField("aggregationColumns");
          columnsField.setAccessible(true);
          columns = String.valueOf(columnsField.get(mode) != null);
          break;
        } catch (NoSuchFieldException e) {
          // not in this class
        }
      }
      return "processingMode=" + mode.getClass().getSimpleName() + " aggregationColumns=" + columns;
    } catch (ReflectiveOperationException e) {
      return "processingMode=?";
    }
  }

  /** The value at row i as a long at the given scale. */
  static long longAt(ColumnVector c, int i, int scale) {
    int row = c.isRepeating ? 0 : i;
    if (!c.noNulls && c.isNull[row]) {
      throw new IllegalStateException("unexpected NULL aggregate");
    }
    if (c instanceof LongColumnVector l) {
      return l.vector[row];
    }
    return ((DecimalColumnVector) c).vector[row].serialize64(scale);
  }

  static String dec(long v) {
    HiveDecimalWritable w = new HiveDecimalWritable();
    w.setFromLongAndScale(v, 2);
    return w.toString();
  }

  static long checksum(Map<String, String> m) {
    long c = 1125899906842597L;
    for (Map.Entry<String, String> e : m.entrySet()) {
      c = 31 * c + e.getKey().hashCode();
      c = 31 * c + e.getValue().hashCode();
    }
    return c;
  }
}
