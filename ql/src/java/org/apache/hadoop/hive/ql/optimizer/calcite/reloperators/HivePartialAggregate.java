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
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Map-side-only aggregate: it is translated to a hash GROUP BY with no shuffle and no merge step.
 *
 * <p>Rows are aggregated only within a task (and within a hash table flush), so the output may hold
 * several rows per group. An aggregate above it must merge the partial results. Hence the group keys
 * are not unique and the row count is the input row count.
 */
public class HivePartialAggregate extends HiveAggregate {

  public HivePartialAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, traitSet, child, groupSet, groupSets, aggCalls);
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new HivePartialAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return mq.getRowCount(getInput());
  }
}
