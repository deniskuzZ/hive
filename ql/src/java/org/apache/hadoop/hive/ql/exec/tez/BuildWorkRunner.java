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

package org.apache.hadoop.hive.ql.exec.tez;

import java.util.Collections;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.runtime.api.Reader;

/**
 * Runs the build work of a map join input (BuildAggregateFusion) on that input, as ReduceRecordProcessor runs a
 * reducer: every record goes through the work's operators, and every row its ReduceSink emits goes to the collector,
 * serialized as the shuffle into the join would have carried it.
 */
public final class BuildWorkRunner {

  private BuildWorkRunner() {
  }

  /**
   * @param memoryAvailable the memory the work's operators may use, for the GROUP BY's hash table and spills
   */
  public static void run(ReduceWork work, JobConf jconf, Reader reader, OutputCollector<?, ?> collector,
      long memoryAvailable) throws Exception {
    Operator<?> root = work.getReducer();
    root.setParentOperators(null);
    root.getConf().setMaxMemoryAvailable(memoryAvailable);
    OperatorUtils.setMemoryAvailable(root.getChildOperators(), memoryAvailable);

    ReduceRecordSource source = new ReduceRecordSource();
    source.init(jconf, root, work.getVectorMode(), work.getKeyDesc(), work.getTagToValueDesc().get(0), reader,
        true, (byte) 0, work.getVectorizedRowBatchCtx(), work.getVectorizedVertexNum(),
        work.getVectorizedTestingReducerBatchSize());
    root.initialize(jconf, new ObjectInspector[] {source.getObjectInspector()});
    OperatorUtils.setChildrenCollector(Collections.singletonList(root), collector);

    boolean done = false;
    try {
      while (source.pushRecord()) {
        // every record is pushed through the work's operators
      }
      done = true;
    } finally {
      boolean closed = source.close();
      // the GROUP BY emits its groups at close: the build is complete only once the work is closed
      root.close(!(done && closed));
    }
  }
}
