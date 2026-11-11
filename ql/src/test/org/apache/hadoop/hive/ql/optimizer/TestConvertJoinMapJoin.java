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

package org.apache.hadoop.hive.ql.optimizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.physical.LlapClusterStateForCompile;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.junit.jupiter.api.Test;

class TestConvertJoinMapJoin {

  @Test
  void testComputeOnlineDataSizeGenericLargeDataSize() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(1000L, Long.MAX_VALUE, 0L, 0L);

    long result = converter.computeOnlineDataSizeGeneric(stats, 10L, 8L);

    assertTrue(result >= 0, "Result should not be negative due to overflow");
  }

  @Test
  void testComputeOnlineDataSizeGenericLargeNumRowsWithOverhead() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(Long.MAX_VALUE / 2, 1000L, 0L, 0L);

    long result = converter.computeOnlineDataSizeGeneric(stats, Long.MAX_VALUE / 4, Long.MAX_VALUE / 4);

    assertTrue(result >= 0, "Result should not be negative due to overflow");
    assertEquals(Long.MAX_VALUE, result, "Result should saturate at Long.MAX_VALUE");
  }

  @Test
  void testComputeOnlineDataSizeGenericNumNullsLargerThanNumRows() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(100L, 10000L, 0L, 0L);
    List<ColStatistics> colStats = new ArrayList<>();
    ColStatistics cs = new ColStatistics("col1", "string");
    cs.setNumNulls(Long.MAX_VALUE);
    colStats.add(cs);
    stats.setColumnStats(colStats);

    long result = converter.computeOnlineDataSizeGeneric(stats, 10L, 8L);

    assertTrue(result >= 0, "Result should not be negative due to underflow in nonNullCount");
  }

  @Test
  void testComputeOnlineDataSizeGenericSmallDataSizeLargeAdjustment() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(1000000L, 100L, 0L, 0L);
    List<ColStatistics> colStats = new ArrayList<>();
    ColStatistics cs = new ColStatistics("col1", "string");
    cs.setNumNulls(0L);
    colStats.add(cs);
    stats.setColumnStats(colStats);

    long result = converter.computeOnlineDataSizeGeneric(stats, 10L, 8L);

    assertTrue(result >= 0, "Result should not be negative when adjustment > dataSize");
  }

  @Test
  void testComputeOnlineDataSizeGenericAllExtremeValues() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(Long.MAX_VALUE, Long.MAX_VALUE, 0L, 0L);
    List<ColStatistics> colStats = new ArrayList<>();
    ColStatistics cs = new ColStatistics("col1", "string");
    cs.setNumNulls(Long.MAX_VALUE);
    colStats.add(cs);
    stats.setColumnStats(colStats);

    long result = converter.computeOnlineDataSizeGeneric(stats, Long.MAX_VALUE, Long.MAX_VALUE);

    assertTrue(result >= 0, "Result should not be negative with extreme values");
    assertEquals(Long.MAX_VALUE, result, "Result should saturate at Long.MAX_VALUE");
  }

  @Test
  void testComputeCumulativeCardinalityWithParentsOverflow() {
    Operator<?> parent1 = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(parent1.getParentOperators()).thenReturn(Collections.emptyList());
    Operator<?> parent2 = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(parent2.getParentOperators()).thenReturn(Collections.emptyList());
    Operator<?> mockOp = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(mockOp.getParentOperators()).thenReturn(Arrays.asList(parent1, parent2));

    Long result = invokeComputeCumulativeCardinality(mockOp);

    assertNotNull(result, "Result should not be null");
    assertTrue(result >= 0, "Result should not be negative due to overflow");
    assertEquals(Long.MAX_VALUE, result.longValue(), "Result should saturate at Long.MAX_VALUE");
  }

  @Test
  void testComputeCumulativeCardinalityDeepTreeOverflow() {
    Operator<?> leaf = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(leaf.getParentOperators()).thenReturn(Collections.emptyList());
    Operator<?> mid1 = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(mid1.getParentOperators()).thenReturn(Collections.singletonList(leaf));
    Operator<?> mid2 = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(mid2.getParentOperators()).thenReturn(Collections.singletonList(mid1));
    Operator<?> root = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(root.getParentOperators()).thenReturn(Collections.singletonList(mid2));

    Long result = invokeComputeCumulativeCardinality(root);

    assertNotNull(result, "Result should not be null");
    assertTrue(result >= 0, "Result should not be negative due to overflow");
    assertEquals(Long.MAX_VALUE, result.longValue(), "Result should saturate at Long.MAX_VALUE");
  }

  // TPC-DS SF10000 estimates at 12 LLAP nodes: q6 broadcasts 65M customer x address rows into a 456M-row probe, q25
  // 1.6M rows into a 1.66B-row one
  @Test
  void testPartitionedJoinWhenTheBroadcastShipsMore() {
    assertTrue(partitionedJoinCostsLess(conf(true), join(456_000_000L, 7_210_000_000L, 65_000_000L, 5_940_000_000L)));
  }

  @Test
  void testBroadcastWhenTheProbeIsLarger() {
    assertFalse(partitionedJoinCostsLess(conf(true), join(10_000_000_000L, 1_000_000_000_000L, 30_000_000L, 3_000_000_000L)));
  }

  @Test
  void testBroadcastWithinTheConfiguredSize() {
    assertFalse(partitionedJoinCostsLess(conf(true), join(1_600_000L, 400_000_000L, 1_600_000L, 840_000_000L)));
  }

  @Test
  void testBroadcastWhenDisabled() {
    assertFalse(partitionedJoinCostsLess(conf(false), join(456_000_000L, 7_210_000_000L, 65_000_000L, 5_940_000_000L)));
    HiveConf noDphj = conf(true);
    noDphj.setBoolVar(HiveConf.ConfVars.HIVE_DYNAMIC_PARTITION_HASHJOIN, false);
    assertFalse(partitionedJoinCostsLess(noDphj, join(456_000_000L, 7_210_000_000L, 65_000_000L, 5_940_000_000L)));
  }

  // the registry was never read: no node count, so the join stays broadcast instead of failing the compile
  @Test
  void testBroadcastWithoutClusterInfo() {
    LlapClusterStateForCompile unread = new LlapClusterStateForCompile(new HiveConf(), 1000L);
    assertFalse(partitionedJoinCostsLess(conf(true), join(456_000_000L, 7_210_000_000L, 65_000_000L, 5_940_000_000L),
        unread));
  }

  private static boolean partitionedJoinCostsLess(HiveConf conf, JoinOperator join) {
    LlapClusterStateForCompile llap = mock(LlapClusterStateForCompile.class);
    when(llap.hasClusterInfo()).thenReturn(true);
    when(llap.getNumExecutorsPerNode()).thenReturn(12);
    when(llap.getKnownExecutorCount()).thenReturn(144);
    return partitionedJoinCostsLess(conf, join, llap);
  }

  private static boolean partitionedJoinCostsLess(HiveConf conf, JoinOperator join, LlapClusterStateForCompile llap) {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    return converter.partitionedJoinCostsLess(conf, join, 0, llap);
  }

  private static HiveConf conf(boolean partitionedByCost) {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_PARTITIONED_BY_COST, partitionedByCost);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_DYNAMIC_PARTITION_HASHJOIN, true);
    conf.setLongVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONAL_TASK_THRESHOLD, 1_431_655_765L);
    return conf;
  }

  /** A join of a big input (position 0) and one small input, with the given estimates. */
  private static JoinOperator join(long bigRows, long bigBytes, long smallRows, long smallBytes) {
    Operator<?> big = mock(Operator.class);
    when(big.getStatistics()).thenReturn(new Statistics(bigRows, bigBytes, 0L, 0L));
    Operator<?> small = mock(Operator.class);
    when(small.getStatistics()).thenReturn(new Statistics(smallRows, smallBytes, 0L, 0L));
    JoinOperator join = mock(JoinOperator.class);
    doReturn(Arrays.asList(big, small)).when(join).getParentOperators();
    return join;
  }

  @SuppressWarnings("unchecked")
  private Operator<?> createMockOperatorWithStats(long numRows) {
    Operator<?> mockOp = mock(Operator.class);
    Statistics stats = new Statistics(numRows, numRows * 100, 0L, 0L);
    when(mockOp.getStatistics()).thenReturn(stats);
    return mockOp;
  }

  private Long invokeComputeCumulativeCardinality(Operator<?> op) {
    try {
      Method method = ConvertJoinMapJoin.class.getDeclaredMethod(
          "computeCumulativeCardinality", Operator.class);
      method.setAccessible(true);
      return (Long) method.invoke(null, op);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
