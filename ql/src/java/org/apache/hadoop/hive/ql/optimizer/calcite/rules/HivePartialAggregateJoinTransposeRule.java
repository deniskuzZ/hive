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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntUnaryOperator;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HivePartialAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;

/**
 * Pushes a {@link HivePartialAggregate} below an inner join, to the join input that holds every aggregate
 * argument. The matched aggregate stays above the join and merges the partial results.
 *
 * <p>Port of Trino's {@code PushPartialAggregationThroughJoin}:
 * <ul>
 *   <li>the join is INNER; an aggregate over a project is matched only if every project expression is
 *   deterministic and references one join input at most;</li>
 *   <li>an aggregate without arguments goes to the larger input (Trino's probe side);</li>
 *   <li>the pushed keys are the original keys available on that input plus its columns used by the join
 *   condition;</li>
 *   <li>the push is skipped when the join expands its input (more than 1.1x), when the pushed keys
 *   outnumber the original keys or are empty, or when a pushed key has NDV x 2 &gt; input rows; missing
 *   statistics also skip it;</li>
 *   <li>unlike Trino, the push is also skipped when the join reduces the pushed input by more than
 *   {@code maxJoinReduction}: Trino filters the probe side at the source with dynamic filters, Hive has none
 *   for a map join, so the pushed aggregate would process every row the join discards.</li>
 * </ul>
 *
 * <p>The pushed aggregate is translated to a map-side hash GROUP BY with no shuffle, so the join's existing
 * shuffle (or map join) consumes it. When the matched aggregate is itself partial it is kept above the join
 * only if the pushed keys are not a subset of its keys.
 *
 * <p>Only SUM, $SUM0, COUNT, MIN and MAX are pushed: their partial result has the final type and is merged
 * by SUM (counts) or the function itself.
 */
public final class HivePartialAggregateJoinTransposeRule extends RelOptRule {

  private static final double MAX_JOIN_EXPANSION = 1.1;

  private final double maxJoinReduction;

  /** Matches an aggregate directly over a join. */
  public static HivePartialAggregateJoinTransposeRule overJoin(double maxJoinReduction) {
    return new HivePartialAggregateJoinTransposeRule(
        operand(HiveAggregate.class, operand(HiveJoin.class, any())),
        "HivePartialAggregateJoinTransposeRule", maxJoinReduction);
  }

  /** Matches an aggregate over a project over a join. */
  public static HivePartialAggregateJoinTransposeRule overProject(double maxJoinReduction) {
    return new HivePartialAggregateJoinTransposeRule(
        operand(HiveAggregate.class, operand(HiveProject.class, operand(HiveJoin.class, any()))),
        "HivePartialAggregateJoinTransposeRule(Project)", maxJoinReduction);
  }

  private HivePartialAggregateJoinTransposeRule(RelOptRuleOperand operand, String description,
      double maxJoinReduction) {
    super(operand, HiveRelFactories.HIVE_BUILDER, description);
    this.maxJoinReduction = maxJoinReduction;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveAggregate aggregate = call.rel(0);
    final Project project = call.rels.length == 3 ? call.rel(1) : null;
    final HiveJoin join = call.rel(call.rels.length - 1);
    if (join.getJoinType() != JoinRelType.INNER || !isSupported(aggregate)) {
      return;
    }

    // Aggregate input expressed over the join output
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final List<RexNode> exprs = project != null ? project.getProjects()
        : rexBuilder.identityProjects(join.getRowType());
    final int leftCount = join.getLeft().getRowType().getFieldCount();
    final int joinCount = join.getRowType().getFieldCount();
    final ImmutableBitSet leftBits = ImmutableBitSet.range(0, leftCount);
    final ImmutableBitSet rightBits = ImmutableBitSet.range(leftCount, joinCount);
    for (RexNode e : exprs) {
      ImmutableBitSet bits = RelOptUtil.InputFinder.bits(e);
      if (!RexUtil.isDeterministic(e) || !(leftBits.contains(bits) || rightBits.contains(bits))) {
        return;
      }
    }

    ImmutableBitSet.Builder argBits = ImmutableBitSet.builder();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      aggCall.getArgList().forEach(arg -> argBits.addAll(RelOptUtil.InputFinder.bits(exprs.get(arg))));
    }
    final RelMetadataQuery mq = call.getMetadataQuery();
    final ImmutableBitSet argFields = argBits.build();
    final int side;
    if (argFields.isEmpty()) {
      side = isLarger(mq, join.getRight(), join.getLeft()) ? 1 : 0;
    } else if (leftBits.contains(argFields)) {
      side = 0;
    } else if (rightBits.contains(argFields)) {
      side = 1;
    } else {
      return;
    }
    final ImmutableBitSet sideBits = side == 0 ? leftBits : rightBits;
    final int sideOffset = side == 0 ? 0 : leftCount;
    final RelNode sideInput = join.getInput(side).stripped();
    if (isPushed(sideInput)) {
      return;
    }

    // Pushed keys: the original keys available on the chosen side, then its join columns
    final IntUnaryOperator toSide = i -> i - sideOffset;
    final List<RexNode> pushedKeys = new ArrayList<>();
    final List<Integer> keyToPushed = new ArrayList<>();
    for (int key : aggregate.getGroupSet()) {
      RexNode e = exprs.get(key);
      ImmutableBitSet bits = RelOptUtil.InputFinder.bits(e);
      keyToPushed.add(!bits.isEmpty() && sideBits.contains(bits) ? addKey(pushedKeys, shift(e, toSide)) : -1);
    }
    final int keysFromGroupBy = pushedKeys.size();
    final int[] joinFieldToPushed = new int[joinCount];
    for (int field : RelOptUtil.InputFinder.bits(join.getCondition()).intersect(sideBits)) {
      joinFieldToPushed[field] = addKey(pushedKeys, rexBuilder.makeInputRef(sideInput, field - sideOffset));
    }
    if (pushedKeys.isEmpty() || pushedKeys.size() > aggregate.getGroupCount()) {
      return;
    }

    // Pushed aggregate: keys first, then the aggregate arguments
    final List<RexNode> belowExprs = new ArrayList<>(pushedKeys);
    final List<AggregateCall> pushedCalls = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      List<Integer> args = new ArrayList<>();
      for (int arg : aggCall.getArgList()) {
        args.add(belowExprs.size());
        belowExprs.add(shift(exprs.get(arg), toSide));
      }
      pushedCalls.add(aggCall.withArgList(args));
    }
    final List<String> belowNames = new ArrayList<>();
    for (int i = 0; i < belowExprs.size(); i++) {
      belowNames.add(belowExprs.get(i) instanceof RexInputRef ref
          ? sideInput.getRowType().getFieldNames().get(ref.getIndex()) : "$f" + i);
    }
    final RelNode below = project(sideInput, belowExprs, SqlValidatorUtil.uniquify(belowNames, true));

    if (!isWorthPushing(mq, join, sideInput, below, pushedKeys.size())) {
      return;
    }
    final int keyCount = pushedKeys.size();
    final HivePartialAggregate pushed = new HivePartialAggregate(aggregate.getCluster(), aggregate.getTraitSet(),
        below, ImmutableBitSet.range(keyCount), null, pushedCalls);

    // New join: the pushed aggregate replaces the chosen input
    final int pushedCount = pushed.getRowType().getFieldCount();
    final int pushedOffset = side == 0 ? 0 : leftCount;
    final IntUnaryOperator joinFieldMapping = i -> {
      if (sideBits.get(i)) {
        return pushedOffset + joinFieldToPushed[i];
      }
      return side == 0 ? i - leftCount + pushedCount : i;
    };
    final RelNode newJoin = join.copy(join.getTraitSet(), shift(join.getCondition(), joinFieldMapping),
        side == 0 ? pushed : join.getLeft(), side == 0 ? join.getRight() : pushed,
        join.getJoinType(), join.isSemiJoinDone());

    // Aggregate input above the join: the original keys, then the partial results
    final List<RexNode> aboveExprs = new ArrayList<>();
    int k = 0;
    for (int key : aggregate.getGroupSet()) {
      int pushedKey = keyToPushed.get(k++);
      aboveExprs.add(pushedKey >= 0 ? rexBuilder.makeInputRef(newJoin, pushedOffset + pushedKey)
          : shift(exprs.get(key), joinFieldMapping));
    }
    for (int i = 0; i < pushedCalls.size(); i++) {
      aboveExprs.add(rexBuilder.makeInputRef(newJoin, pushedOffset + keyCount + i));
    }
    final List<String> names = aggregate.getRowType().getFieldNames();
    final RelNode above = project(newJoin, aboveExprs, names);

    final boolean keepAggregate = !(aggregate instanceof HivePartialAggregate) || keyCount > keysFromGroupBy;
    call.transformTo(keepAggregate ? mergeAggregate(aggregate, above) : above);
  }

  private static boolean isSupported(Aggregate aggregate) {
    if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
      return false;
    }
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (aggCall.isDistinct() || aggCall.hasFilter() || !aggCall.getCollation().getFieldCollations().isEmpty()) {
        return false;
      }
      switch (aggCall.getAggregation().getKind()) {
      case SUM, SUM0, COUNT, MIN, MAX -> { }
      default -> {
        return false;
      }
      }
    }
    return true;
  }

  /**
   * Trino pushes an aggregate without arguments to the left input, which is its probe side there. Hive's
   * join inputs are not ordered that way, so the larger input stands for the probe side.
   */
  private static boolean isLarger(RelMetadataQuery mq, RelNode rel, RelNode other) {
    Double rows = mq.getRowCount(rel);
    Double otherRows = mq.getRowCount(other);
    return isKnown(rows) && isKnown(otherRows) && rows > otherRows;
  }

  /**
   * Whether the aggregate was pushed into this join input already: a partial aggregate is reachable through
   * projects and joins only (a partial aggregate that made an earlier one redundant replaced it by a join).
   */
  private static boolean isPushed(RelNode rel) {
    rel = rel.stripped();
    if (rel instanceof HivePartialAggregate) {
      return true;
    }
    if (rel instanceof Project p) {
      return isPushed(p.getInput());
    }
    return rel instanceof HiveJoin j && (isPushed(j.getLeft()) || isPushed(j.getRight()));
  }

  /** Trino's statistics gates plus the join reduction gate; missing statistics skip the push. */
  private boolean isWorthPushing(RelMetadataQuery mq, HiveJoin join, RelNode sideInput, RelNode below,
      int keyCount) {
    Double sideRows = mq.getRowCount(sideInput);
    Double joinRows = mq.getRowCount(join);
    if (!isKnown(sideRows) || !isKnown(joinRows) || joinRows > MAX_JOIN_EXPANSION * sideRows) {
      return false;
    }
    if (maxJoinReduction > 0 && joinRows * maxJoinReduction < sideRows) {
      return false;
    }
    for (int i = 0; i < keyCount; i++) {
      Double ndv = mq.getDistinctRowCount(below, ImmutableBitSet.of(i), null);
      if (!isKnown(ndv) || ndv * 2 > sideRows) {
        return false;
      }
    }
    return true;
  }

  private static boolean isKnown(Double d) {
    return d != null && !d.isNaN();
  }

  /**
   * Rebuilds the matched aggregate over the partial results. COUNT and $SUM0 are merged by SUM; it never
   * sees an empty group because the aggregate has at least one key. A merged decimal SUM is cast back to
   * the original type.
   */
  private static RelNode mergeAggregate(HiveAggregate aggregate, RelNode input) {
    final RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();
    final int keyCount = aggregate.getGroupCount();
    final List<AggregateCall> mergeCalls = new ArrayList<>();
    boolean needsCast = false;
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      AggregateCall aggCall = aggregate.getAggCallList().get(i);
      int arg = keyCount + i;
      RelDataType argType = input.getRowType().getFieldList().get(arg).getType();
      AggregateCall mergeCall = switch (aggCall.getAggregation().getKind()) {
      case MIN, MAX -> aggCall.withArgList(List.of(arg));
      case COUNT, SUM0 -> sum(arg, argType, aggCall.getType(), aggCall.getName());
      default -> {
        RelDataType sumType = typeFactory.getTypeSystem().deriveSumType(typeFactory, argType);
        if (SqlTypeUtil.equalSansNullability(typeFactory, sumType, aggCall.getType())) {
          sumType = aggCall.getType();
        } else {
          needsCast = true;
        }
        yield sum(arg, argType, sumType, aggCall.getName());
      }
      };
      mergeCalls.add(mergeCall);
    }
    final HiveAggregate merged = (HiveAggregate) aggregate.copy(aggregate.getTraitSet(), input,
        ImmutableBitSet.range(keyCount), null, mergeCalls);
    merged.setAggregateColumnsOrder(aggregate.getAggregateColumnsOrder());
    if (!needsCast) {
      return merged;
    }
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final List<RexNode> casts = new ArrayList<>();
    for (int i = 0; i < merged.getRowType().getFieldCount(); i++) {
      RexNode ref = rexBuilder.makeInputRef(merged, i);
      RelDataType type = aggregate.getRowType().getFieldList().get(i).getType();
      casts.add(SqlTypeUtil.equalSansNullability(typeFactory, ref.getType(), type) ? ref
          : rexBuilder.makeCast(type, ref, true, false));
    }
    return project(merged, casts, aggregate.getRowType().getFieldNames());
  }

  private static RelNode project(RelNode input, List<RexNode> exprs, List<String> names) {
    RelDataType rowType = RexUtil.createStructType(input.getCluster().getTypeFactory(), exprs, names, null);
    return HiveProject.create(input.getCluster(), input, exprs, rowType, List.of());
  }

  private static AggregateCall sum(int arg, RelDataType argType, RelDataType type, String name) {
    SqlAggFunction sum = SqlFunctionConverter.getCalciteAggFn("sum", List.of(argType), type);
    return AggregateCall.create(sum, false, false, false, List.of(), List.of(arg), -1, null,
        RelCollations.EMPTY, type, name);
  }

  private static int addKey(List<RexNode> keys, RexNode key) {
    int pos = keys.indexOf(key);
    if (pos < 0) {
      keys.add(key);
      pos = keys.size() - 1;
    }
    return pos;
  }

  private static RexNode shift(RexNode e, IntUnaryOperator mapping) {
    return e.accept(new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef ref) {
        return new RexInputRef(mapping.applyAsInt(ref.getIndex()), ref.getType());
      }
    });
  }
}
