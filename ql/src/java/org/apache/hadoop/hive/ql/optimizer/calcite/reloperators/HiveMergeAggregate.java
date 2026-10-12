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

package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Aggregate that merges the results of a {@link HivePartialAggregate}: SUM of partial sums and counts, MIN and MAX
 * of partial minimums and maximums, each typed as the aggregate it completes. It is translated to a GROUP BY whose
 * evaluators run in merge modes (PARTIAL2 on the map side, FINAL in the reducer), which keep the partial result
 * type; SUM, MIN and MAX compute the same in merge and in input modes.
 */
public class HiveMergeAggregate extends HiveAggregate {

  public HiveMergeAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, traitSet, child, groupSet, groupSets, aggCalls);
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new HiveMergeAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
  }
}
