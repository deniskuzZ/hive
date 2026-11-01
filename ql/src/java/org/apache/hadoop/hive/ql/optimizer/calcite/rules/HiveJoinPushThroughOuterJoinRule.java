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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

/**
 * Moves an inner join below the LEFT outer join that is its left input when the inner join's condition does not
 * reference the outer join's null-generating side: (A LEFT JOIN B) JOIN C becomes (A JOIN C) LEFT JOIN B under a
 * project that restores the column order (outer join associativity, Moerkotte and Neumann's l-asscom).
 * <p>
 * JoinToMultiJoinRule never combines a multi-join that contains an outer join, so join ordering keeps the outer join
 * below every inner join above it. TPC-DS q80's store_sales LEFT JOIN store_returns then reads the whole of
 * store_sales, and its date, item and promotion joins filter only the outer join's output. Moved below, they filter
 * store_sales first, join its other inner joins in one multi-join, and the semijoin from store_sales into
 * store_returns is sized from the filtered rows.
 * <p>
 * Only an inner join that does not increase A's estimated rows is moved: an expanding join below the outer join would
 * enlarge its input.
 */
public final class HiveJoinPushThroughOuterJoinRule extends RelOptRule {

  public static final HiveJoinPushThroughOuterJoinRule INSTANCE = new HiveJoinPushThroughOuterJoinRule();

  private HiveJoinPushThroughOuterJoinRule() {
    super(operand(HiveJoin.class, operand(HiveJoin.class, any()), operand(RelNode.class, any())),
        HiveRelFactories.HIVE_BUILDER, "HiveJoinPushThroughOuterJoinRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Join top = call.rel(0);
    final Join outer = call.rel(1);
    final RelNode c = call.rel(2);
    if (top.getJoinType() != JoinRelType.INNER || outer.getJoinType() != JoinRelType.LEFT) {
      return;
    }
    final RelNode a = outer.getLeft();
    final RelNode b = outer.getRight();
    final int aCount = a.getRowType().getFieldCount();
    final int bCount = b.getRowType().getFieldCount();
    final int cCount = c.getRowType().getFieldCount();
    if (RelOptUtil.InputFinder.bits(top.getCondition()).intersects(ImmutableBitSet.range(aCount, aCount + bCount))) {
      return;
    }

    // top's fields are A | B | C, the inner join's A | C, the outer join's A | C | B
    final RexBuilder rexBuilder = top.getCluster().getRexBuilder();
    final RexNode innerCondition = shift(rexBuilder, top.getCondition(), aCount + bCount, -bCount);
    final Join inner = top.copy(top.getTraitSet(), innerCondition, a, c, JoinRelType.INNER, top.isSemiJoinDone());
    final RelMetadataQuery mq = call.getMetadataQuery();
    // a PK-FK join that keeps every row of A only appends C's columns: it filters nothing below the outer join, and
    // HiveJoinSwapConstraintsRule, in the same rule collection, moves such a join back above the outer join
    if (HiveRelOptUtil.isRewritablePKFKJoin(inner, a, c, mq).rewritable) {
      return;
    }
    final Double innerRows = mq.getRowCount(inner);
    final Double aRows = mq.getRowCount(a);
    if (innerRows == null || aRows == null || innerRows > aRows) {
      return;
    }
    final RexNode outerCondition = shift(rexBuilder, outer.getCondition(), aCount, cCount);
    final Join moved = outer.copy(outer.getTraitSet(), outerCondition, inner, b, JoinRelType.LEFT,
        outer.isSemiJoinDone());

    final List<RexNode> fields = new ArrayList<>(aCount + bCount + cCount);
    for (int i = 0; i < aCount; i++) {
      fields.add(rexBuilder.makeInputRef(moved, i));
    }
    for (int i = 0; i < bCount; i++) {
      fields.add(rexBuilder.makeInputRef(moved, aCount + cCount + i));
    }
    for (int i = 0; i < cCount; i++) {
      fields.add(rexBuilder.makeInputRef(moved, aCount + i));
    }
    final RelBuilder relBuilder = call.builder();
    call.transformTo(relBuilder.push(moved).project(fields, top.getRowType().getFieldNames()).build());
  }

  /** The condition with every input reference at or above {@code from} moved by {@code offset}. */
  private static RexNode shift(RexBuilder rexBuilder, RexNode condition, int from, int offset) {
    return condition.accept(new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef ref) {
        return ref.getIndex() < from ? ref : rexBuilder.makeInputRef(ref.getType(), ref.getIndex() + offset);
      }
    });
  }
}
