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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMinus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pushes a map-side (hash) GROUP BY below an inner join, to the join input that holds every aggregation
 * argument. The pushed GROUP BY runs in {@link GroupByDesc.Mode#PARTIAL_HASH} mode in the vertex of that input,
 * before its existing shuffle (or map join). The matched GROUP BY stays above the join with the same mode and
 * output, but its aggregations merge the partial results ({@code PARTIAL2}) instead of iterating over rows.
 * <pre>
 *   TS - SEL - RS \                                 TS - SEL - GBY(partialhash) - RS \
 *                  JOIN - SEL - GBY(hash) - RS  =>                                    JOIN - GBY(hash) - RS
 *   TS - SEL - RS /                                 TS - SEL ------------------ RS /
 * </pre>
 * With {@code hive.optimize.partial.aggr.join.transpose.merge.above.join} off, a Select projecting the keys and
 * the partial results replaces the matched GROUP BY, and only the reduce-side GROUP BY merges them:
 * <pre>
 *   TS - SEL - GBY(partialhash) - RS \
 *                                     JOIN - SEL - RS - GBY(mergepartial)
 *   TS - SEL ------------------ RS /
 * </pre>
 *
 * <p>Criteria must meet
 * <ul>
 *   <li>the join is a common (shuffle) join and every join condition is INNER; each join input is a
 *   ReduceSink;</li>
 *   <li>the GROUP BY is over the join through Select operators only; it has keys, no grouping sets and no
 *   distinct aggregation, and only SUM, $SUM0, COUNT, MIN and MAX aggregations: their partial result is a plain
 *   value, merged by SUM (counts) or the function itself;</li>
 *   <li>every key and aggregation argument is deterministic and references one join input at most, or the argument
 *   is an IF or CASE whose condition references one join input and whose value the other (TPC-DS q2, q43:
 *   {@code sum(if(d_day_name = 'Sunday', ss_sales_price, null))}): the value is aggregated below the join and the
 *   condition, a function of the other input's row, is applied to the partial result above it. Every row of a pushed
 *   group has the same join key, so it joins the same rows of the other input. The value is aggregated over the rows
 *   whose condition is false too, so it must not fail on them: columns, constants, +, -, * and casts. The ELSE is
 *   NULL, or 0 for a SUM;</li>
 *   <li>the GROUP BY is pushed to the input holding every aggregation argument; an aggregation without
 *   arguments goes to the larger input (example: count(*));</li>
 *   <li>the pushed keys are the original keys available on that input plus its join keys and its columns used
 *   by residual join filters;</li>
 *   <li>the push is skipped when the join expands its input by more than
 *   {@code hive.optimize.partial.aggr.join.transpose.max.join.expansion}, when the pushed keys outnumber the
 *   original keys and the columns of the IF conditions or are empty, or when a pushed key has an NDV of more than
 *   {@code hive.optimize.partial.aggr.join.transpose.max.key.ndv.ratio} x input rows; missing statistics also
 *   skip it;</li>
 *   <li>the push is also skipped when the join reduces the pushed input by more than
 *   {@code hive.optimize.partial.aggr.join.transpose.max.join.reduction}: Hive has no dynamic filters for a map
 *   join, so the pushed GROUP BY would process every row the join discards.</li>
 * </ul>
 *
 * <p>A pushed GROUP BY is matched again, so it can move further down a chain of joins. It is not kept above the
 * join it moves below: a Select projecting its keys and the partial results replaces it.
 *
 * <p>The criteria need operator statistics, so this runs in {@code TezCompiler} after a statistics annotation.
 * It runs before the top n key optimization: a Top N Key operator pushed down to the join output would sit between
 * the join and the GROUP BY. It runs before the dynamic partition pruning: its semijoin branches would hang off the
 * join inputs and the joins,
 * so the GROUP BY could not move below them, and built afterwards they read from the pushed GROUP BY. The
 * synthetic join predicates of residual join filters that read the values of a pushed join input are moved to the
 * pushed GROUP BY output.
 */
public class PartialGroupByJoinTransposeOptimizer extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(PartialGroupByJoinTransposeOptimizer.class);

  private static final Set<String> SUPPORTED_UDAFS = new HashSet<>(Arrays.asList("sum", "$sum0", "count", "min",
      "max"));

  // inputOf results other than a join input tag
  private static final int NO_INPUT = -1;
  private static final int INVALID = -2;

  private HiveConf conf;
  private float minReduction;
  private float maxJoinReduction;
  private float maxJoinExpansion;
  private float maxKeyNdvRatio;
  private boolean mergeAboveJoin;
  // the value lists of synthetic join predicates from residual join filters: their target is an expression over
  // the input of the join input they read from
  private final List<ExprNodeDynamicListDesc> residualDynamicLists = new ArrayList<>();

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    conf = pctx.getConf();
    minReduction = HiveConf.getFloatVar(conf, HiveConf.ConfVars.PARTIAL_AGGR_JOIN_TRANSPOSE_MIN_REDUCTION);
    maxJoinReduction = HiveConf.getFloatVar(conf, HiveConf.ConfVars.PARTIAL_AGGR_JOIN_TRANSPOSE_MAX_JOIN_REDUCTION);
    maxJoinExpansion = HiveConf.getFloatVar(conf, HiveConf.ConfVars.PARTIAL_AGGR_JOIN_TRANSPOSE_MAX_JOIN_EXPANSION);
    maxKeyNdvRatio = HiveConf.getFloatVar(conf, HiveConf.ConfVars.PARTIAL_AGGR_JOIN_TRANSPOSE_MAX_KEY_NDV_RATIO);
    mergeAboveJoin = HiveConf.getBoolVar(conf, HiveConf.ConfVars.PARTIAL_AGGR_JOIN_TRANSPOSE_MERGE_ABOVE_JOIN);

    String JOIN = JoinOperator.getOperatorName() + "%";
    String SEL = SelectOperator.getOperatorName() + "%";
    String GBY = GroupByOperator.getOperatorName() + "%";

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<>();
    opRules.put(new RuleRegExp("R1", JOIN + "(" + SEL + ")*" + GBY), new CandidateCollector());

    // collect the candidates first: the rewrite changes the operators around the join
    CandidateCtx ctx = new CandidateCtx();
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, ctx);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    List<Node> topNodes = new ArrayList<>(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    if (ctx.candidates.isEmpty()) {
      return pctx;
    }

    List<Operator<?>> topOps = new ArrayList<>(pctx.getTopOps().values());
    for (FilterOperator filter : OperatorUtils.findOperators(topOps, FilterOperator.class)) {
      collectResidualDynamicLists(filter.getConf().getPredicate(), residualDynamicLists);
    }

    Deque<GroupByOperator> worklist = new ArrayDeque<>(ctx.candidates);
    while (!worklist.isEmpty()) {
      GroupByOperator pushed = push(worklist.poll());
      if (pushed != null) {
        // it may move further down a chain of joins
        worklist.add(pushed);
      }
    }
    return pctx;
  }

  private static void collectResidualDynamicLists(ExprNodeDesc expr, List<ExprNodeDynamicListDesc> lists) {
    if (expr instanceof ExprNodeDynamicListDesc) {
      if (((ExprNodeDynamicListDesc) expr).getTarget() != null) {
        lists.add((ExprNodeDynamicListDesc) expr);
      }
    } else if (expr != null && expr.getChildren() != null) {
      for (ExprNodeDesc child : expr.getChildren()) {
        collectResidualDynamicLists(child, lists);
      }
    }
  }

  private static class CandidateCtx implements NodeProcessorCtx {
    private final Set<GroupByOperator> candidates = new LinkedHashSet<>();
  }

  private static class CandidateCollector implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) {
      ((CandidateCtx) procCtx).candidates.add((GroupByOperator) nd);
      return null;
    }
  }

  /**
   * Pushes the GROUP BY below the join it is over, if every criterion is met.
   *
   * @return the pushed GROUP BY, or null if the plan is unchanged
   */
  private GroupByOperator push(GroupByOperator gby) throws SemanticException {
    GroupByDesc gbyDesc = gby.getConf();
    if (gby.getNumParent() != 1 || !gbyDesc.getMode().isHash() || gbyDesc.isGroupingSetsPresent()
        || gbyDesc.getBucketGroup() || gbyDesc.isDistinct() || gbyDesc.getKeys().isEmpty()) {
      return null;
    }
    List<AggregationDesc> aggs = gbyDesc.getAggregators();
    for (AggregationDesc agg : aggs) {
      if (agg.getDistinct() || agg.getMode() != GenericUDAFEvaluator.Mode.PARTIAL1
          || !SUPPORTED_UDAFS.contains(agg.getGenericUDAFName().toLowerCase())) {
        return null;
      }
    }

    // the operator directly over the join: the lowest Select or the GROUP BY itself
    Operator<?> bottom = gby;
    Operator<?> parent = gby.getParentOperators().get(0);
    while (parent instanceof SelectOperator) {
      if (parent.getNumChild() != 1 || parent.getNumParent() != 1) {
        return null;
      }
      bottom = parent;
      parent = parent.getParentOperators().get(0);
    }
    if (!(parent instanceof JoinOperator) || parent.getNumChild() != 1) {
      return null;
    }
    JoinOperator join = (JoinOperator) parent;
    JoinDesc joinDesc = join.getConf();
    if (joinDesc.getHandleSkewJoin()) {
      return null;
    }
    for (JoinCondDesc cond : joinDesc.getConds()) {
      if (cond.getType() != JoinDesc.INNER_JOIN) {
        return null;
      }
    }
    ReduceSinkOperator[] rsByTag = new ReduceSinkOperator[join.getNumParent()];
    for (Operator<?> joinParent : join.getParentOperators()) {
      if (!(joinParent instanceof ReduceSinkOperator) || joinParent.getNumParent() != 1
          || joinParent.getNumChild() != 1) {
        return null;
      }
      int tag = ((ReduceSinkOperator) joinParent).getConf().getTag();
      if (tag < 0 || tag >= rsByTag.length || rsByTag[tag] != null) {
        return null;
      }
      rsByTag[tag] = (ReduceSinkOperator) joinParent;
    }
    Map<String, Byte> reversedExprs = joinDesc.getReversedExprs();
    if (reversedExprs == null || join.getColumnExprMap() == null) {
      return null;
    }
    for (String name : joinDesc.getOutputColumnNames()) {
      if (reversedExprs.get(name) == null || join.getSchema().getColumnInfo(name) == null) {
        return null;
      }
    }

    // keys and aggregation arguments over the join output, and the join input they reference
    List<ExprNodeDesc> keys = ExprNodeDescUtils.backtrack(gbyDesc.getKeys(), gby, bottom);
    int[] keyInputs = new int[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      keyInputs[i] = inputOf(keys.get(i), reversedExprs);
      if (keyInputs[i] == INVALID) {
        return null;
      }
    }
    List<List<ExprNodeDesc>> params = new ArrayList<>();
    List<CaseSplit> splits = new ArrayList<>();
    Set<String> conditionCols = new HashSet<>();
    int side = NO_INPUT;
    for (AggregationDesc agg : aggs) {
      List<ExprNodeDesc> aggParams = ExprNodeDescUtils.backtrack(agg.getParameters(), gby, bottom);
      CaseSplit split = null;
      if (aggParams.size() == 1 && inputOf(aggParams.get(0), reversedExprs) == INVALID) {
        // the value is aggregated below the join, on the input other than the condition's
        split = splitCase(agg, aggParams.get(0), reversedExprs);
        if (split == null) {
          return null;
        }
        conditionCols.addAll(split.conditionCols);
        aggParams = new ArrayList<>(Arrays.asList(split.value));
      }
      splits.add(split);
      for (ExprNodeDesc param : aggParams) {
        int input = inputOf(param, reversedExprs);
        if (input == INVALID || input != NO_INPUT && side != NO_INPUT && input != side) {
          return null;
        }
        if (input != NO_INPUT) {
          side = input;
        }
      }
      params.add(aggParams);
    }
    if (side == NO_INPUT) {
      side = largestInput(rsByTag);
      if (side == NO_INPUT) {
        return null;
      }
    }
    if (joinDesc.getFilters() != null && joinDesc.getFilters().get((byte) side) != null
        && !joinDesc.getFilters().get((byte) side).isEmpty()) {
      return null;
    }
    ReduceSinkOperator rs = rsByTag[side];
    ReduceSinkDesc rsDesc = rs.getConf();
    Operator<?> below = rs.getParentOperators().get(0);

    // pushed keys over the input of the join input: the original keys of the input, its join keys and its
    // columns used by residual filters
    List<ExprNodeDesc> pushedKeys = new ArrayList<>();
    int[] keyToPushed = new int[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      keyToPushed[i] = -1;
      if (keyInputs[i] == side) {
        ExprNodeDesc key = ExprNodeDescUtils.backtrack(keys.get(i), bottom, rs);
        if (key == null) {
          return null;
        }
        keyToPushed[i] = addUnique(pushedKeys, key);
      }
    }
    List<ExprNodeDesc> rsKeyCols = rsDesc.getKeyCols();
    int[] rsKeyToPushed = new int[rsKeyCols.size()];
    for (int k = 0; k < rsKeyCols.size(); k++) {
      rsKeyToPushed[k] = addUnique(pushedKeys, rsKeyCols.get(k));
    }
    List<ExprNodeDesc> residualFilters = joinDesc.getResidualFilterExprs();
    Map<String, Integer> filterColToPushed = new HashMap<>();
    if (residualFilters != null) {
      for (ExprNodeDesc filter : residualFilters) {
        List<ExprNodeColumnDesc> cols = columnsOf(filter);
        if (cols == null) {
          return null;
        }
        for (ExprNodeColumnDesc col : cols) {
          Byte tag = reversedExprs.get(col.getColumn());
          if (tag == null) {
            return null;
          }
          if (tag == side && !filterColToPushed.containsKey(col.getColumn())) {
            ExprNodeDesc key = ExprNodeDescUtils.backtrack(col, bottom, rs);
            if (key == null) {
              return null;
            }
            filterColToPushed.put(col.getColumn(), addUnique(pushedKeys, key));
          }
        }
      }
    }
    // the columns of the IF conditions split the rows of the GROUP BY as its keys do (Trino's
    // PreAggregateCaseAggregations groups by them)
    if (pushedKeys.isEmpty() || pushedKeys.size() > keys.size() + conditionCols.size()) {
      return null;
    }
    List<ExprNodeDesc> partitionCols = rsDesc.getPartitionCols();
    int[] partitionToPushed = new int[partitionCols.size()];
    for (int k = 0; k < partitionCols.size(); k++) {
      partitionToPushed[k] = ExprNodeDescUtils.indexOf(partitionCols.get(k), pushedKeys);
      if (partitionToPushed[k] < 0) {
        return null;
      }
    }
    // the synthetic join predicates of residual filters reading from the join input read from the pushed keys
    List<ExprNodeDynamicListDesc> dynamicLists = new ArrayList<>();
    List<Integer> dynamicListToPushed = new ArrayList<>();
    for (ExprNodeDynamicListDesc dynamicList : residualDynamicLists) {
      if (dynamicList.getSource() == rs) {
        int p = ExprNodeDescUtils.indexOf(dynamicList.getTarget(), pushedKeys);
        if (p < 0) {
          return null;
        }
        dynamicLists.add(dynamicList);
        dynamicListToPushed.add(p);
      }
    }

    if (!isWorthPushing(join, below, pushedKeys)) {
      return null;
    }

    // the pushed GROUP BY: the pushed keys, then the partial results
    int numPushedKeys = pushedKeys.size();
    List<String> pushedNames = new ArrayList<>();
    List<ColumnInfo> pushedCols = new ArrayList<>();
    Map<String, ExprNodeDesc> pushedColExprMap = new HashMap<>();
    for (int p = 0; p < numPushedKeys; p++) {
      String name = SemanticAnalyzer.getColumnInternalName(p);
      pushedNames.add(name);
      pushedCols.add(new ColumnInfo(name, pushedKeys.get(p).getTypeInfo(), "", false));
      pushedColExprMap.put(name, pushedKeys.get(p));
    }
    List<AggregationDesc> pushedAggs = new ArrayList<>();
    for (int a = 0; a < aggs.size(); a++) {
      List<ExprNodeDesc> aggParams = new ArrayList<>();
      for (ExprNodeDesc param : params.get(a)) {
        ExprNodeDesc pushedParam = ExprNodeDescUtils.backtrack(param, bottom, rs);
        if (pushedParam == null) {
          return null;
        }
        aggParams.add(pushedParam);
      }
      String udafName = aggs.get(a).getGenericUDAFName();
      GenericUDAFEvaluator evaluator = FunctionRegistry.getGenericUDAFEvaluator(udafName,
          aggParams.stream().map(ExprNodeDesc::getWritableObjectInspector).collect(Collectors.toList()),
          false, aggParams.isEmpty());
      if (evaluator == null) {
        return null;
      }
      GenericUDAFInfo udafInfo =
          SemanticAnalyzer.getGenericUDAFInfo(evaluator, GenericUDAFEvaluator.Mode.PARTIAL1, aggParams);
      pushedAggs.add(new AggregationDesc(udafName, udafInfo.genericUDAFEvaluator, udafInfo.convertedParameters,
          false, GenericUDAFEvaluator.Mode.PARTIAL1));
      String name = SemanticAnalyzer.getColumnInternalName(pushedNames.size());
      pushedNames.add(name);
      pushedCols.add(new ColumnInfo(name, udafInfo.returnType, "", false));
    }
    GroupByDesc pushedDesc = new GroupByDesc(GroupByDesc.Mode.PARTIAL_HASH, pushedNames, pushedKeys, pushedAggs,
        false, gbyDesc.getGroupByMemoryUsage(), gbyDesc.getMemoryThreshold(),
        Math.min(gbyDesc.getMinReductionHashAggr(), minReduction), gbyDesc.getMinReductionHashAggrLowerBound(),
        gbyDesc.getHashAggrFlushPercent(), null, false, -1, false);

    // the join input: the pushed keys used as join keys stay keys, the other outputs of the pushed GROUP BY
    // become values
    int[] pushedToRsKey = new int[numPushedKeys];
    Arrays.fill(pushedToRsKey, -1);
    for (int k = 0; k < rsKeyToPushed.length; k++) {
      if (pushedToRsKey[rsKeyToPushed[k]] < 0) {
        pushedToRsKey[rsKeyToPushed[k]] = k;
      }
    }
    List<String> rsKeyNames = rsDesc.getOutputKeyColumnNames();
    List<ExprNodeDesc> newKeyCols = new ArrayList<>();
    Map<String, ExprNodeDesc> rsColExprMap = new HashMap<>();
    List<ColumnInfo> rsSignature = new ArrayList<>();
    for (int k = 0; k < rsKeyToPushed.length; k++) {
      ExprNodeDesc keyCol = columnRef(pushedCols.get(rsKeyToPushed[k]));
      String field = Utilities.ReduceField.KEY + "." + rsKeyNames.get(k);
      newKeyCols.add(keyCol);
      rsColExprMap.put(field, keyCol);
      rsSignature.add(new ColumnInfo(field, keyCol.getTypeInfo(), "", false));
    }
    List<ExprNodeDesc> newPartitionCols = new ArrayList<>();
    for (int p : partitionToPushed) {
      newPartitionCols.add(columnRef(pushedCols.get(p)));
    }
    ArrayList<ExprNodeDesc> valueCols = new ArrayList<>();
    ArrayList<String> valueNames = new ArrayList<>();
    int[] valueIndex = new int[pushedCols.size()];
    String[] rsFields = new String[pushedCols.size()];
    for (int i = 0; i < pushedCols.size(); i++) {
      if (i < numPushedKeys && pushedToRsKey[i] >= 0) {
        valueIndex[i] = pushedToRsKey[i];
        rsFields[i] = Utilities.ReduceField.KEY + "." + rsKeyNames.get(pushedToRsKey[i]);
      } else {
        ExprNodeDesc valueCol = columnRef(pushedCols.get(i));
        valueIndex[i] = -valueCols.size() - 1;
        rsFields[i] = Utilities.ReduceField.VALUE + "." + pushedNames.get(i);
        valueCols.add(valueCol);
        valueNames.add(pushedNames.get(i));
        rsColExprMap.put(rsFields[i], valueCol);
        rsSignature.add(new ColumnInfo(rsFields[i], valueCol.getTypeInfo(), "", false));
      }
    }

    // the join outputs of the input: the pushed keys used above the join or by residual filters, and the
    // partial results; fresh names, as the outputs of the other inputs keep theirs
    boolean[] usedAbove = new boolean[pushedCols.size()];
    for (int p : keyToPushed) {
      if (p >= 0) {
        usedAbove[p] = true;
      }
    }
    for (int p : filterColToPushed.values()) {
      usedAbove[p] = true;
    }
    Arrays.fill(usedAbove, numPushedKeys, usedAbove.length, true);
    Set<String> otherNames = new HashSet<>();
    for (String name : joinDesc.getOutputColumnNames()) {
      if (reversedExprs.get(name) != side) {
        otherNames.add(name);
      }
    }
    String[] joinNames = new String[pushedCols.size()];
    List<ExprNodeDesc> sideExprs = new ArrayList<>();
    List<String> sideNames = new ArrayList<>();
    List<ColumnInfo> sideCols = new ArrayList<>();
    Map<String, ExprNodeDesc> newJoinColExprMap = new HashMap<>();
    Map<String, Byte> newReversedExprs = new HashMap<>();
    int nextName = 0;
    for (int i = 0; i < pushedCols.size(); i++) {
      if (!usedAbove[i]) {
        continue;
      }
      String name;
      do {
        name = SemanticAnalyzer.getColumnInternalName(nextName++);
      } while (otherNames.contains(name));
      TypeInfo type = pushedCols.get(i).getType();
      ExprNodeDesc expr = new ExprNodeColumnDesc(type, rsFields[i], null, false);
      joinNames[i] = name;
      sideExprs.add(expr);
      sideNames.add(name);
      sideCols.add(new ColumnInfo(name, type, "", false));
      newJoinColExprMap.put(name, expr);
      newReversedExprs.put(name, (byte) side);
    }
    for (Map.Entry<String, ExprNodeDesc> e : join.getColumnExprMap().entrySet()) {
      Byte tag = reversedExprs.get(e.getKey());
      if (tag != null && tag != side) {
        newJoinColExprMap.put(e.getKey(), e.getValue());
      }
    }
    // the join output concatenates the outputs of its inputs in tag order
    List<String> newOutputNames = new ArrayList<>();
    List<ColumnInfo> newJoinSignature = new ArrayList<>();
    for (byte tag : tagOrder(joinDesc, rsByTag.length)) {
      if (tag == side) {
        newOutputNames.addAll(sideNames);
        newJoinSignature.addAll(sideCols);
        continue;
      }
      for (String name : joinDesc.getOutputColumnNames()) {
        if (reversedExprs.get(name) == tag) {
          newOutputNames.add(name);
          newJoinSignature.add(join.getSchema().getColumnInfo(name));
          newReversedExprs.put(name, tag);
        }
      }
    }
    final int sideTag = side;
    Function<ExprNodeColumnDesc, ExprNodeDesc> toNewJoinOutput = col -> {
      if (reversedExprs.get(col.getColumn()) != sideTag) {
        return col;
      }
      int p = filterColToPushed.get(col.getColumn());
      return new ExprNodeColumnDesc(pushedCols.get(p).getType(), joinNames[p], col.getTabAlias(),
          col.getIsPartitionColOrVirtualCol());
    };
    List<ExprNodeDesc> newResidualFilters = null;
    if (residualFilters != null) {
      newResidualFilters = new ArrayList<>();
      for (ExprNodeDesc filter : residualFilters) {
        newResidualFilters.add(substitute(filter, toNewJoinOutput));
      }
    }

    // over the join: keys of the input are its new join outputs, aggregation arguments the partial results
    List<ExprNodeDesc> newKeys = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      newKeys.add(keyToPushed[i] >= 0
          ? new ExprNodeColumnDesc(pushedCols.get(keyToPushed[i]).getType(), joinNames[keyToPushed[i]], null, false)
          : keys.get(i));
    }
    List<ExprNodeDesc> partialRefs = new ArrayList<>();
    for (int a = 0; a < aggs.size(); a++) {
      int i = numPushedKeys + a;
      ExprNodeDesc partial = new ExprNodeColumnDesc(pushedCols.get(i).getType(), joinNames[i], null, false);
      CaseSplit split = splits.get(a);
      if (split != null) {
        // a COUNT whose rows all fail the condition is 0; the other functions keep the ELSE
        TypeInfo type = partial.getTypeInfo();
        boolean count = "count".equalsIgnoreCase(aggs.get(a).getGenericUDAFName());
        ExprNodeConstantDesc otherwise = count || split.zero ? zeroOf(type) : new ExprNodeConstantDesc(type, null);
        if (otherwise == null) {
          return null;
        }
        partial = ExprNodeGenericFuncDesc.newInstance(new GenericUDFIf(),
            new ArrayList<>(Arrays.asList(split.condition.clone(), partial, otherwise)));
      }
      partialRefs.add(partial);
    }

    LOG.debug("Pushing {} below {} to input {}", gby, join, side);

    // splice the pushed GROUP BY between the join input and its input
    GroupByOperator pushed = (GroupByOperator) OperatorFactory.get(below.getCompilationOpContext(), pushedDesc,
        new RowSchema(pushedCols));
    pushed.setColumnExprMap(pushedColExprMap);
    below.replaceChild(rs, pushed);
    pushed.setParentOperators(Utilities.makeList(below));
    pushed.setChildOperators(Utilities.makeList(rs));
    rs.replaceParent(below, pushed);

    rsDesc.setKeyCols(newKeyCols);
    rsDesc.setPartitionCols(newPartitionCols);
    rsDesc.setValueCols(valueCols);
    rsDesc.setOutputValueColumnNames(valueNames);
    rsDesc.setValueSerializeInfo(PlanUtils.getReduceValueTableDesc(
        PlanUtils.getFieldSchemasFromColumnList(valueCols, valueNames, 0, "")));
    rs.setValueIndex(valueIndex);
    rs.setColumnExprMap(rsColExprMap);
    rs.setSchema(new RowSchema(rsSignature));
    for (int i = 0; i < dynamicLists.size(); i++) {
      dynamicLists.get(i).setTarget(columnRef(pushedCols.get(dynamicListToPushed.get(i))));
    }

    Map<Byte, List<ExprNodeDesc>> joinExprs = new LinkedHashMap<>(joinDesc.getExprs());
    joinExprs.put((byte) side, sideExprs);
    joinDesc.setExprs(joinExprs);
    joinDesc.setOutputColumnNames(newOutputNames);
    joinDesc.setReversedExprs(newReversedExprs);
    if (newResidualFilters != null) {
      joinDesc.setResidualFilterExprs(newResidualFilters);
    }
    ExprNodeDesc[][] joinKeys = joinDesc.getJoinKeys();
    if (joinKeys != null && side < joinKeys.length) {
      joinKeys[side] = newKeyCols.toArray(new ExprNodeDesc[0]);
    }
    join.setColumnExprMap(newJoinColExprMap);
    join.setSchema(new RowSchema(newJoinSignature));

    if (gbyDesc.getMode() == GroupByDesc.Mode.HASH && mergeAboveJoin) {
      // the GROUP BY merges the partial results; the Selects between it and the join are dropped
      gbyDesc.setKeys(newKeys);
      Map<String, ExprNodeDesc> gbyColExprMap =
          gby.getColumnExprMap() == null ? new HashMap<>() : new HashMap<>(gby.getColumnExprMap());
      for (int i = 0; i < newKeys.size(); i++) {
        gbyColExprMap.put(gbyDesc.getOutputColumnNames().get(i), newKeys.get(i));
      }
      gby.setColumnExprMap(gbyColExprMap);
      for (int a = 0; a < aggs.size(); a++) {
        aggs.get(a).setParameters(new ArrayList<>(Arrays.asList(partialRefs.get(a))));
        aggs.get(a).setMode(GenericUDAFEvaluator.Mode.PARTIAL2);
      }
      if (bottom != gby) {
        Operator<?> gbyParent = gby.getParentOperators().get(0);
        join.replaceChild(bottom, gby);
        gby.replaceParent(gbyParent, join);
        bottom.setParentOperators(null);
        gbyParent.setChildOperators(null);
      }
    } else {
      // the partial results pass through the join: the reduce-side GROUP BY merges them, or the GROUP BY above the
      // next join if this one was pushed earlier
      List<ExprNodeDesc> selExprs = new ArrayList<>(newKeys);
      selExprs.addAll(partialRefs);
      List<String> selNames = new ArrayList<>(gbyDesc.getOutputColumnNames());
      Map<String, ExprNodeDesc> selColExprMap = new HashMap<>();
      for (int i = 0; i < selNames.size(); i++) {
        selColExprMap.put(selNames.get(i), selExprs.get(i));
      }
      SelectOperator sel = (SelectOperator) OperatorFactory.get(join.getCompilationOpContext(),
          new SelectDesc(selExprs, selNames), new RowSchema(gby.getSchema()));
      sel.setColumnExprMap(selColExprMap);
      join.replaceChild(bottom, sel);
      sel.setParentOperators(Utilities.makeList(join));
      sel.setChildOperators(new ArrayList<>(gby.getChildOperators()));
      for (Operator<?> child : gby.getChildOperators()) {
        child.replaceParent(gby, sel);
      }
      bottom.setParentOperators(null);
      gby.setParentOperators(null);
      gby.setChildOperators(null);
    }
    return pushed;
  }

  /**
   * Cost criteria, from the statistics of the join and of the input of the join input.
   */
  private boolean isWorthPushing(JoinOperator join, Operator<?> below, List<ExprNodeDesc> pushedKeys) {
    Statistics inputStats = below.getStatistics();
    Statistics joinStats = join.getStatistics();
    if (inputStats == null || joinStats == null) {
      return false;
    }
    long inputRows = inputStats.getNumRows();
    long joinRows = joinStats.getNumRows();
    if (joinRows > maxJoinExpansion * inputRows) {
      return false;
    }
    if (maxJoinReduction > 0 && joinRows * maxJoinReduction < inputRows) {
      return false;
    }
    for (ExprNodeDesc key : pushedKeys) {
      ColStatistics keyStats = StatsUtils.getColStatisticsFromExpression(conf, inputStats, key);
      if (keyStats == null || keyStats.getCountDistint() > maxKeyNdvRatio * inputRows) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return the tag of the join input whose output columns the expression references, NO_INPUT if it
   *         references none, or INVALID if it cannot be pushed
   */
  private static int inputOf(ExprNodeDesc expr, Map<String, Byte> reversedExprs) {
    if (expr == null || !ExprNodeDescUtils.isDeterministic(expr)) {
      return INVALID;
    }
    List<ExprNodeColumnDesc> cols = columnsOf(expr);
    if (cols == null) {
      return INVALID;
    }
    int input = NO_INPUT;
    for (ExprNodeColumnDesc col : cols) {
      Byte tag = reversedExprs.get(col.getColumn());
      if (tag == null || input != NO_INPUT && input != tag) {
        return INVALID;
      }
      input = tag;
    }
    return input;
  }

  /** An aggregation argument IF(condition, value, otherwise) split around the join. */
  private static final class CaseSplit {
    private final ExprNodeDesc condition;
    // cast to the type of the IF, so the partial result has the type of the original one
    private final ExprNodeDesc value;
    // the ELSE is 0, otherwise NULL
    private final boolean zero;
    private final Set<String> conditionCols;

    private CaseSplit(ExprNodeDesc condition, ExprNodeDesc value, boolean zero, Set<String> conditionCols) {
      this.condition = condition;
      this.value = value;
      this.zero = zero;
      this.conditionCols = conditionCols;
    }
  }

  /**
   * The argument as an IF or CASE that can be split around the join, or null: IF(condition, value, otherwise) with
   * the condition on one join input and the value on the other. The value is aggregated over rows whose condition
   * is false too, so it must not fail on them: columns, constants, +, -, * and casts. Otherwise is NULL, or 0 for a
   * SUM, which adds nothing; a COUNT counts the rows whose condition holds and whose value is not NULL.
   */
  private static CaseSplit splitCase(AggregationDesc agg, ExprNodeDesc param, Map<String, Byte> reversedExprs)
      throws SemanticException {
    if (!(param instanceof ExprNodeGenericFuncDesc) || !ExprNodeDescUtils.isDeterministic(param)) {
      return null;
    }
    GenericUDF udf = ((ExprNodeGenericFuncDesc) param).getGenericUDF();
    List<ExprNodeDesc> children = param.getChildren();
    boolean when = udf instanceof GenericUDFWhen && (children.size() == 2 || children.size() == 3);
    if (!when && !(udf instanceof GenericUDFIf && children.size() == 3)) {
      return null;
    }
    ExprNodeDesc condition = children.get(0);
    ExprNodeDesc value = children.get(1);
    int conditionInput = inputOf(condition, reversedExprs);
    int valueInput = inputOf(value, reversedExprs);
    if (conditionInput < 0 || valueInput < 0 || conditionInput == valueInput || !cannotFail(value)) {
      return null;
    }
    String udafName = agg.getGenericUDAFName().toLowerCase();
    boolean zero = false;
    if (children.size() == 3 && !isNull(children.get(2))) {
      if (!("sum".equals(udafName) || "$sum0".equals(udafName)) || !isZero(children.get(2))) {
        return null;
      }
      zero = true;
    }
    if (!value.getTypeInfo().equals(param.getTypeInfo())) {
      if (!(param.getTypeInfo() instanceof PrimitiveTypeInfo)) {
        return null;
      }
      value = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .createConversionCast(value, (PrimitiveTypeInfo) param.getTypeInfo());
    }
    Set<String> conditionCols = new HashSet<>();
    for (ExprNodeColumnDesc col : columnsOf(condition)) {
      conditionCols.add(col.getColumn());
    }
    return new CaseSplit(condition, value, zero, conditionCols);
  }

  private static boolean cannotFail(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeColumnDesc || expr instanceof ExprNodeConstantDesc) {
      return true;
    }
    if (!(expr instanceof ExprNodeGenericFuncDesc)) {
      return false;
    }
    GenericUDF udf = ((ExprNodeGenericFuncDesc) expr).getGenericUDF();
    if (!(udf instanceof GenericUDFOPPlus || udf instanceof GenericUDFOPMinus || udf instanceof GenericUDFOPMultiply
        || FunctionRegistry.isOpNegative(expr) || FunctionRegistry.isOpCast(expr))) {
      return false;
    }
    for (ExprNodeDesc child : expr.getChildren()) {
      if (!cannotFail(child)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isNull(ExprNodeDesc expr) {
    return expr instanceof ExprNodeConstantDesc && ((ExprNodeConstantDesc) expr).getValue() == null;
  }

  private static boolean isZero(ExprNodeDesc expr) {
    if (!(expr instanceof ExprNodeConstantDesc)) {
      return false;
    }
    Object value = ((ExprNodeConstantDesc) expr).getValue();
    return value instanceof HiveDecimal ? ((HiveDecimal) value).signum() == 0
        : value instanceof Number && ((Number) value).doubleValue() == 0;
  }

  /** A zero of a SUM or COUNT partial result type, or null for another type. */
  private static ExprNodeConstantDesc zeroOf(TypeInfo type) {
    if (!(type instanceof PrimitiveTypeInfo)) {
      return null;
    }
    switch (((PrimitiveTypeInfo) type).getPrimitiveCategory()) {
    case LONG:
      return new ExprNodeConstantDesc(type, 0L);
    case DOUBLE:
      return new ExprNodeConstantDesc(type, 0.0d);
    case DECIMAL:
      return new ExprNodeConstantDesc(type, HiveDecimal.ZERO);
    default:
      return null;
    }
  }

  /** The join input with the most rows, or NO_INPUT without statistics. */
  private static int largestInput(ReduceSinkOperator[] rsByTag) {
    int largest = NO_INPUT;
    long largestRows = -1;
    for (int tag = 0; tag < rsByTag.length; tag++) {
      Statistics stats = rsByTag[tag].getStatistics();
      if (stats == null) {
        return NO_INPUT;
      }
      if (stats.getNumRows() > largestRows) {
        largest = tag;
        largestRows = stats.getNumRows();
      }
    }
    return largest;
  }

  private static Byte[] tagOrder(JoinDesc joinDesc, int numInputs) {
    if (joinDesc.getTagOrder() != null) {
      return joinDesc.getTagOrder();
    }
    Byte[] tagOrder = new Byte[numInputs];
    for (int tag = 0; tag < numInputs; tag++) {
      tagOrder[tag] = (byte) tag;
    }
    return tagOrder;
  }

  private static int addUnique(List<ExprNodeDesc> exprs, ExprNodeDesc expr) {
    int index = ExprNodeDescUtils.indexOf(expr, exprs);
    if (index >= 0) {
      return index;
    }
    exprs.add(expr);
    return exprs.size() - 1;
  }

  private static ExprNodeDesc columnRef(ColumnInfo col) {
    return new ExprNodeColumnDesc(col.getType(), col.getInternalName(), null, false);
  }

  /** The referenced columns, or null if the expression has an unsupported kind of node. */
  private static List<ExprNodeColumnDesc> columnsOf(ExprNodeDesc expr) {
    List<ExprNodeColumnDesc> cols = new ArrayList<>();
    return substitute(expr, col -> {
      cols.add(col);
      return col;
    }) == null ? null : cols;
  }

  /**
   * Copies the expression with its columns replaced.
   *
   * @return null if the expression has an unsupported kind of node
   */
  private static ExprNodeDesc substitute(ExprNodeDesc expr, Function<ExprNodeColumnDesc, ExprNodeDesc> columns) {
    if (expr instanceof ExprNodeColumnDesc) {
      return columns.apply((ExprNodeColumnDesc) expr);
    }
    if (expr instanceof ExprNodeConstantDesc) {
      return expr;
    }
    if (expr instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) expr.clone();
      List<ExprNodeDesc> children = new ArrayList<>();
      for (ExprNodeDesc child : func.getChildren()) {
        ExprNodeDesc newChild = substitute(child, columns);
        if (newChild == null) {
          return null;
        }
        children.add(newChild);
      }
      func.setChildren(children);
      return func;
    }
    if (expr instanceof ExprNodeFieldDesc) {
      ExprNodeFieldDesc field = (ExprNodeFieldDesc) expr.clone();
      ExprNodeDesc desc = substitute(field.getDesc(), columns);
      if (desc == null) {
        return null;
      }
      field.setDesc(desc);
      return field;
    }
    return null;
  }
}
