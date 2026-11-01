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

import java.util.regex.Pattern;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimestampString;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;

/**
 * Compares a DATE cast to TIMESTAMP with TIMESTAMP literals as a DATE (Trino's UnwrapCastInComparison):
 * CAST(d AS TIMESTAMP) op t becomes d op' DATE, and SEARCH(CAST(d AS TIMESTAMP), ranges), the form a BETWEEN or a
 * pair of comparisons takes, becomes SEARCH(d, the ranges in days). A date casts to its midnight, so the cast is
 * injective and order-preserving and the result stays the same, NULLs included. A literal with a time of day moves a
 * bound to the date it falls in (d < t holds for t's own date); an equality with it matches no date, so a point drops
 * out of a SEARCH and a comparison with = or <> is left as it is.
 * <p>
 * The predicate is then on the column, so its range statistics apply. TPC-DS writes date ranges as
 * d_date BETWEEN date AND date + N days, and date + interval is a TIMESTAMP (q5, q12, q16, q20, q21, q32, q37, q40,
 * q77, q80, q82, q92, q94, q95, q98).
 */
public final class HiveUnwrapCastInComparisonRule extends RelOptRule {

  public static final HiveUnwrapCastInComparisonRule INSTANCE = new HiveUnwrapCastInComparisonRule();

  private static final Pattern MIDNIGHT = Pattern.compile("00:00:00(\\.0*)?");
  private static final Pattern DATE_PART = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

  private HiveUnwrapCastInComparisonRule() {
    super(operand(HiveFilter.class, any()), HiveRelFactories.HIVE_BUILDER, "HiveUnwrapCastInComparisonRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RexNode condition = filter.getCondition().accept(new RexShuttle() {
      @Override
      public RexNode visitCall(RexCall call) {
        final RexNode visited = super.visitCall(call);
        if (!(visited instanceof RexCall c)) {
          return visited;
        }
        return switch (c.getKind()) {
          case EQUALS, NOT_EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL ->
              unwrapComparison(rexBuilder, c);
          case SEARCH -> unwrapSearch(rexBuilder, c);
          default -> c;
        };
      }
    });
    if (condition != filter.getCondition()) {
      call.transformTo(filter.copy(filter.getTraitSet(), filter.getInput(), condition));
    }
  }

  private static RexNode unwrapComparison(RexBuilder rexBuilder, RexCall call) {
    RexNode date = castDate(call.getOperands().get(0));
    TimestampString t = timestamp(call.getOperands().get(1));
    SqlKind kind = call.getKind();
    if (date == null || t == null) {
      date = castDate(call.getOperands().get(1));
      t = timestamp(call.getOperands().get(0));
      kind = kind.reverse();
    }
    if (date == null || t == null) {
      return call;
    }
    final DateString day = dayOf(t);
    final boolean midnight = isMidnight(t);
    // with a time of day: d < t and d <= t hold up to t's date, d > t and d >= t after it
    final SqlOperator op = switch (kind) {
      case EQUALS -> midnight ? SqlStdOperatorTable.EQUALS : null;
      case NOT_EQUALS -> midnight ? SqlStdOperatorTable.NOT_EQUALS : null;
      case LESS_THAN -> midnight ? SqlStdOperatorTable.LESS_THAN : SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      case LESS_THAN_OR_EQUAL -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      case GREATER_THAN -> SqlStdOperatorTable.GREATER_THAN;
      case GREATER_THAN_OR_EQUAL ->
          midnight ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.GREATER_THAN;
      default -> null;
    };
    if (op == null || day == null) {
      return call;
    }
    return rexBuilder.makeCall(op, date, rexBuilder.makeDateLiteral(day));
  }

  @SuppressWarnings("unchecked")
  private static RexNode unwrapSearch(RexBuilder rexBuilder, RexCall call) {
    final RexNode date = castDate(call.getOperands().get(0));
    if (date == null || !(call.getOperands().get(1) instanceof RexLiteral literal)
        || literal.getType().getSqlTypeName() != SqlTypeName.TIMESTAMP) {
      return call;
    }
    final Sarg<TimestampString> sarg = literal.getValueAs(Sarg.class);
    // two ranges of one date's timestamps become the same date: the set merges them
    final RangeSet<DateString> days = TreeRangeSet.create();
    for (Range<TimestampString> range : sarg.rangeSet.asRanges()) {
      final Range<DateString> inDays = inDays(range);
      if (inDays == null) {
        return call;
      }
      if (!inDays.isEmpty()) {
        days.add(inDays);
      }
    }
    return rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, date,
        rexBuilder.makeSearchArgumentLiteral(Sarg.of(sarg.nullAs, days), date.getType()));
  }

  /**
   * The dates whose midnight is in a range of timestamps (empty for a point with a time of day), or null for a bound
   * outside the yyyy-MM-dd years. A lower bound t with a time of day starts at the next date; an upper one, open or
   * closed, ends at t's date, closed.
   */
  private static Range<DateString> inDays(Range<TimestampString> range) {
    DateString lower = null;
    BoundType lowerType = BoundType.CLOSED;
    if (range.hasLowerBound()) {
      lower = dayOf(range.lowerEndpoint());
      if (lower == null) {
        return null;
      }
      if (range.lowerBoundType() == BoundType.OPEN) {
        lowerType = BoundType.OPEN;
      } else if (!isMidnight(range.lowerEndpoint())) {
        lower = DateString.fromDaysSinceEpoch(lower.getDaysSinceEpoch() + 1);
      }
    }
    DateString upper = null;
    BoundType upperType = BoundType.CLOSED;
    if (range.hasUpperBound()) {
      upper = dayOf(range.upperEndpoint());
      if (upper == null) {
        return null;
      }
      if (range.upperBoundType() == BoundType.OPEN && isMidnight(range.upperEndpoint())) {
        upperType = BoundType.OPEN;
      }
    }
    if (lower == null) {
      return upper == null ? Range.all() : Range.upTo(upper, upperType);
    }
    if (upper == null) {
      return Range.downTo(lower, lowerType);
    }
    final int c = lower.compareTo(upper);
    if (c > 0 || c == 0 && (lowerType == BoundType.OPEN || upperType == BoundType.OPEN)) {
      return Range.closedOpen(lower, lower);
    }
    return Range.range(lower, lowerType, upper, upperType);
  }

  /** The DATE operand of CAST(date AS TIMESTAMP), or null. */
  private static RexNode castDate(RexNode node) {
    if (node.getKind() != SqlKind.CAST || node.getType().getSqlTypeName() != SqlTypeName.TIMESTAMP) {
      return null;
    }
    final RexNode operand = ((RexCall) node).getOperands().get(0);
    return operand.getType().getSqlTypeName() == SqlTypeName.DATE ? operand : null;
  }

  /** The value of a non-NULL TIMESTAMP literal, or null. */
  private static TimestampString timestamp(RexNode node) {
    return node instanceof RexLiteral literal && literal.getTypeName() == SqlTypeName.TIMESTAMP
        ? literal.getValueAs(TimestampString.class) : null;
  }

  /** The date a timestamp falls in, or null outside the yyyy-MM-dd years. */
  private static DateString dayOf(TimestampString t) {
    final String s = t.toString();
    return s.length() > 10 && DATE_PART.matcher(s.substring(0, 10)).matches() ? new DateString(s.substring(0, 10))
        : null;
  }

  private static boolean isMidnight(TimestampString t) {
    final String s = t.toString();
    return s.length() > 11 && MIDNIGHT.matcher(s.substring(11)).matches();
  }
}
