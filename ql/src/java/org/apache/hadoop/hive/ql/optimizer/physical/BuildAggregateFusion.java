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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinCommonOperator;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicValueDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the final GROUP BY that only feeds the build input of a co-partitioned bucket map join in the join's task
 * (Trino: exchange, final aggregation and hash builder in one stage).
 * ConvertJoinMapJoin#convertJoinCoPartitioned partitions the build input into the join's vertex with the hash of the
 * join's stream input. When that input is the output of an unsorted GROUP BY on the join keys, the GROUP BY's own
 * input, its partial aggregates, is hashed by the same keys, so it can be partitioned into the join's vertex instead:
 * the GROUP BY's work is attached to the map join for that input and its input edge moves to the join's vertex. The
 * hash table loader runs the work there, merging the partials, and loads the rows the work's ReduceSink emits
 * (VectorMapJoinFastHashTableLoader), so the aggregate's rows are not shuffled a second time.
 */
public class BuildAggregateFusion implements PhysicalPlanResolver {
  private static final Logger LOG = LoggerFactory.getLogger(BuildAggregateFusion.class);

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    for (TezTask task : Utilities.getTezTasks(pctx.getRootTasks())) {
      fuse(task.getWork());
    }
    return pctx;
  }

  private static void fuse(TezWork tezWork) {
    Map<String, BaseWork> works = new HashMap<>();
    for (BaseWork work : tezWork.getAllWork()) {
      works.put(work.getName(), work);
    }
    for (BaseWork work : new ArrayList<>(works.values())) {
      if (!(work instanceof ReduceWork join) || join.getTagToInput().size() != 1) {
        continue;
      }
      ReduceSinkDesc partitioner = reduceSinkInto(works.get(join.getTagToInput().values().iterator().next()), join);
      if (partitioner == null) {
        continue;
      }
      for (MapJoinOperator mapJoin : OperatorUtils.findOperators(join.getAllRootOperators(), MapJoinOperator.class)) {
        MapJoinDesc desc = mapJoin.getConf();
        if (!desc.isBucketMapJoin() || !(mapJoin instanceof VectorMapJoinCommonOperator vectorJoin)
            || ((VectorMapJoinDesc) vectorJoin.getVectorDesc()).getHashTableImplementationType()
                != HashTableImplementationType.FAST) {
          // only the fast vectorized hash table loader runs a build work
          continue;
        }
        for (Map.Entry<Integer, String> input : new ArrayList<>(desc.getParentToInput().entrySet())) {
          if (input.getKey() != desc.getPosBigTable()
              && works.get(input.getValue()) instanceof ReduceWork build
              && fusible(tezWork, build, join, partitioner)) {
            BaseWork producer = tezWork.getParents(build).get(0);
            ReduceSinkDesc partial = reduceSinkInto(producer, build);
            ReduceSinkDesc small = buildReduceSink(build);
            TezEdgeProperty edge = tezWork.getEdgeProperty(build, join);
            tezWork.disconnect(producer, build);
            tezWork.disconnect(build, join);
            tezWork.remove(build);
            works.remove(build.getName());
            tezWork.connect(producer, join, edge);
            // the partials are partitioned into the join's partitions as the build input was
            partial.setOutputName(join.getName());
            partial.setReducerTraits(EnumSet.copyOf(small.getReducerTraits()));
            partial.setNumReducers(small.getNumReducers());
            desc.getParentToInput().put(input.getKey(), producer.getName());
            desc.getParentToBuildWork().put(input.getKey(), build);
            LOG.info("{} of {} runs in the hash table loader of {} on {}", build.getName(), mapJoin, join.getName(),
                producer.getName());
          }
        }
      }
    }
  }

  /**
   * Whether the build input is the output of a vectorized, unsorted, final GROUP BY on the join keys that feeds nothing
   * else, through Select and Filter operators only, and whether its input is partitioned by the hash of the join's
   * stream input: Murmur3 over the same serialized keys (UNIFORM), with the same key types, order and null order.
   */
  private static boolean fusible(TezWork tezWork, ReduceWork build, ReduceWork join, ReduceSinkDesc partitioner) {
    List<BaseWork> producers = tezWork.getParents(build);
    if (producers.size() != 1 || tezWork.getChildren(build).size() != 1
        || tezWork.getParents(join).contains(producers.get(0))
        || tezWork.getEdgeProperty(build, join).getEdgeType() != EdgeType.CUSTOM_SIMPLE_EDGE
        || tezWork.getEdgeProperty(producers.get(0), build).getEdgeType() != EdgeType.CUSTOM_SIMPLE_EDGE
        || !build.getVectorMode() || build.getLlapMode() != join.getLlapMode()
        || build.getDummyOps() != null && !build.getDummyOps().isEmpty()
        || build.getTagToValueDesc().size() != 1 || build.getAllRootOperators().size() != 1) {
      return false;
    }
    Operator<?> root = build.getReducer();
    if (!(root.getConf() instanceof GroupByDesc groupBy) || groupBy.getMode() != GroupByDesc.Mode.MERGEPARTIAL
        || !groupBy.isUnsortedInput() || groupBy.isDistinct() || groupBy.isGroupingSetsPresent()
        || groupBy.getKeys().isEmpty()) {
      return false;
    }
    ReduceSinkDesc small = buildReduceSink(build);
    ReduceSinkDesc partial = reduceSinkInto(producers.get(0), build);
    if (small == null || partial == null || !join.getName().equals(small.getOutputName())) {
      return false;
    }
    // the build input's keys are the GROUP BY's keys, in order
    List<String> keys = groupByKeysOf(root, small);
    if (keys == null || !keys.equals(groupBy.getOutputColumnNames().subList(0, groupBy.getKeys().size()))) {
      return false;
    }
    // the GROUP BY's keys are its input's ReduceSink keys, in order
    List<String> partialKeys = partial.getOutputKeyColumnNames();
    if (partialKeys.size() != groupBy.getKeys().size()) {
      return false;
    }
    for (int i = 0; i < partialKeys.size(); i++) {
      if (!(groupBy.getKeys().get(i) instanceof ExprNodeColumnDesc key)
          || !key.getColumn().equals(Utilities.ReduceField.KEY + "." + partialKeys.get(i))) {
        return false;
      }
    }
    if (!partial.getReducerTraits().contains(ReducerTraits.UNIFORM) || partial.getTopN() >= 0
        || !ExprNodeDescUtils.isSame(partial.getKeyCols(), partial.getPartitionCols())
        || partial.getKeyCols().size() != partitioner.getKeyCols().size()
        || !partial.getOrder().equals(partitioner.getOrder())
        || !partial.getNullOrder().equals(partitioner.getNullOrder())) {
      return false;
    }
    for (int i = 0; i < partial.getKeyCols().size(); i++) {
      if (!partial.getKeyCols().get(i).getTypeInfo().equals(partitioner.getKeyCols().get(i).getTypeInfo())) {
        return false;
      }
    }
    return true;
  }

  /**
   * The GROUP BY output columns the build input's key columns come from, through the Select and Filter operators
   * between them, or null if a key is computed or another operator is in the way.
   */
  private static List<String> groupByKeysOf(Operator<?> groupBy, ReduceSinkDesc small) {
    List<Operator<?>> chain = new ArrayList<>();
    Operator<?> op = groupBy;
    while (!op.getIsReduceSink()) {
      if (op.getChildOperators() == null || op.getChildOperators().size() != 1) {
        return null;
      }
      op = op.getChildOperators().get(0);
      if (op.getConf() instanceof FilterDesc filter) {
        if (hasDynamicValue(filter.getPredicate())) {
          // a runtime filter waits on a source of the GROUP BY's vertex, not of the join's
          return null;
        }
      } else if (!op.getIsReduceSink() && !(op.getConf() instanceof SelectDesc)) {
        return null;
      }
      chain.add(op);
    }
    List<String> keys = new ArrayList<>();
    for (ExprNodeDesc key : small.getKeyCols()) {
      if (!(key instanceof ExprNodeColumnDesc column)) {
        return null;
      }
      String name = column.getColumn();
      for (int i = chain.size() - 2; i >= 0 && name != null; i--) {
        if (chain.get(i).getConf() instanceof SelectDesc select && !select.isSelStarNoCompute()) {
          int at = select.getOutputColumnNames().indexOf(name);
          name = at >= 0 && select.getColList().get(at) instanceof ExprNodeColumnDesc from ? from.getColumn() : null;
        }
      }
      if (name == null) {
        return null;
      }
      keys.add(name);
    }
    return keys;
  }

  private static boolean hasDynamicValue(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeDynamicValueDesc) {
      return true;
    }
    if (expr.getChildren() != null) {
      for (ExprNodeDesc child : expr.getChildren()) {
        if (hasDynamicValue(child)) {
          return true;
        }
      }
    }
    return false;
  }

  /** The ReduceSink that ends the build work: the one operator of the work without children. */
  private static ReduceSinkDesc buildReduceSink(ReduceWork build) {
    Operator<?> op = build.getReducer();
    while (op.getChildOperators() != null && op.getChildOperators().size() == 1) {
      op = op.getChildOperators().get(0);
    }
    return op.getIsReduceSink() && (op.getChildOperators() == null || op.getChildOperators().isEmpty())
        ? (ReduceSinkDesc) op.getConf() : null;
  }

  /** The ReduceSink of the work whose output is the input of the consumer, or null. */
  private static ReduceSinkDesc reduceSinkInto(BaseWork work, BaseWork consumer) {
    if (work == null) {
      return null;
    }
    for (Operator<?> op : work.getAllOperators()) {
      if (op.getIsReduceSink() && consumer.getName().equals(((ReduceSinkDesc) op.getConf()).getOutputName())) {
        return (ReduceSinkDesc) op.getConf();
      }
    }
    return null;
  }
}
