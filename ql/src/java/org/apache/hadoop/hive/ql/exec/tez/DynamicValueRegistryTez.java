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

package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.DynamicValueRegistry;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.common.NoDynamicValuesException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DynamicValueRegistryTez implements DynamicValueRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicValueRegistryTez.class);

  public static class RegistryConfTez extends RegistryConf {
    public Configuration conf;
    public BaseWork baseWork;
    public ProcessorContext processorContext;
    public Map<String, LogicalInput> inputs;

    public RegistryConfTez(Configuration conf, BaseWork baseWork,
        ProcessorContext processorContext, Map<String, LogicalInput> inputs) {
      super();
      this.conf = conf;
      this.baseWork = baseWork;
      this.processorContext = processorContext;
      this.inputs = inputs;
    }
  }

  static class NullValue {
  }

  static final NullValue NULL_VALUE = new NullValue();

  /** A runtime value source of the task running on this thread. */
  private static final class TaskSource {
    private final String key;
    private final String name;
    private final RuntimeValuesInfo info;
    private final RegistryConfTez conf;
    private boolean started;

    TaskSource(String key, String name, RuntimeValuesInfo info, RegistryConfTez conf) {
      this.key = key;
      this.name = name;
      this.info = info;
      this.conf = conf;
    }

    LogicalInput input() {
      return conf.inputs.get(name);
    }

    TaskSource start() throws Exception {
      if (!started) {
        input().start();
        started = true;
      }
      return this;
    }
  }

  protected Map<String, Object> values = new ConcurrentHashMap<>();
  private final Map<String, Lock> sourceLocks = new ConcurrentHashMap<>();
  private final Set<String> readSources = ConcurrentHashMap.newKeySet();
  // The task that started its input for a source not read yet. One task per source fetches it.
  private final Map<String, TaskSource> readers = new ConcurrentHashMap<>();
  // The sources of the task running on this thread: a task reads its own input only.
  private final ThreadLocal<List<TaskSource>> taskSources = new ThreadLocal<>();

  public DynamicValueRegistryTez() {
  }

  /**
   * The value, or {@link NoDynamicValuesException} while its source has not arrived. On a miss the task of this thread
   * reads the sources whose input is ready, without waiting for the others.
   */
  @Override
  public Object getValue(String key) throws Exception {
    Object val = values.get(key);
    if (val == null) {
      readTaskSources(null);
      val = values.get(key);
    }
    return value(key, val);
  }

  /** The value, waiting for its source; for consumers that cannot pass rows while it is missing. */
  @Override
  public Object waitForValue(String key) throws Exception {
    Object val = values.get(key);
    if (val == null) {
      readTaskSources(key);
      val = values.get(key);
    }
    return value(key, val);
  }

  private static Object value(String key, Object val) {
    if (val == null) {
      throw new NoDynamicValuesException("Value does not exist in registry: " + key);
    }
    return val == NULL_VALUE ? null : val;
  }

  protected void setValue(String key, Object value) {
    if (value == null) {
      // ConcurrentHashMap does not allow null - use a substitute value.
      values.put(key, NULL_VALUE);
    } else {
      values.put(key, value);
    }
  }

  /**
   * Starts reading the runtime values of the task's input sources, without waiting for them: rows pass the runtime
   * filters until their values arrive (see {@link #getValue}). On LLAP the tasks of a query share this registry, so
   * one task of a vertex on a daemon fetches a source and the other tasks use its values: a runtime value can be a
   * large bloom filter, which every task would otherwise fetch from its one producer. If that task ends before its
   * source arrives, the next task that needs the value fetches it. A registry holds the values of one DAG
   * ({@link DynamicValue#registryKey}).
   */
  @Override
  public void init(RegistryConf conf) throws Exception {
    RegistryConfTez rct = (RegistryConfTez) conf;
    List<TaskSource> sources = new ArrayList<>();
    for (Map.Entry<String, RuntimeValuesInfo> source : rct.baseWork.getInputSourceToRuntimeValuesInfo().entrySet()) {
      String key = rct.baseWork.getName() + ":" + source.getKey();
      if (readSources.contains(key)) {
        LOG.info("Runtime value source: {} (already read)", source.getKey());
      } else {
        sources.add(new TaskSource(key, source.getKey(), source.getValue(), rct));
      }
    }
    taskSources.set(sources);
    readTaskSources(null);
  }

  /** Ends the task of this thread: a source it started and did not read goes to the next task that needs it. */
  public void release() {
    List<TaskSource> sources = taskSources.get();
    if (sources != null) {
      for (TaskSource source : sources) {
        readers.remove(source.key, source);
      }
      taskSources.remove();
    }
  }

  /**
   * Reads the sources of this thread's task whose input is ready. A source no task fetches yet is started first.
   * With a key, waits for the source that holds it, starting this task's own input if another task fetches it.
   */
  private void readTaskSources(String waitKey) throws Exception {
    List<TaskSource> sources = taskSources.get();
    if (sources == null) {
      return;
    }
    for (Iterator<TaskSource> it = sources.iterator(); it.hasNext(); ) {
      TaskSource source = it.next();
      if (!readSources.contains(source.key)) {
        boolean wait = waitKey != null && source.info.getDynamicValueIDs().contains(waitKey);
        if (readers.putIfAbsent(source.key, source) == null || wait) {
          source.start();
        }
        if (!source.started) {
          continue;
        }
        List<Input> input = List.of(source.input());
        if (wait) {
          source.conf.processorContext.waitForAllInputsReady(input);
        } else if (!source.conf.processorContext.waitForAllInputsReady(input, 0)) {
          continue;
        }
        readOnce(source);
      }
      it.remove();
    }
  }

  private void readOnce(TaskSource source) throws Exception {
    Lock lock = sourceLocks.computeIfAbsent(source.key, k -> new ReentrantLock());
    lock.lockInterruptibly();
    try {
      if (!readSources.contains(source.key)) {
        read(source.conf, source.name, source.info);
        readSources.add(source.key);
      }
    } finally {
      lock.unlock();
      readers.remove(source.key, source);
    }
  }

  private void read(RegistryConfTez rct, String inputSourceName, RuntimeValuesInfo runtimeValuesInfo)
      throws Exception {
    LOG.info("Runtime value source: " + inputSourceName);

    LogicalInput runtimeValueInput = rct.inputs.get(inputSourceName);

    // Setup deserializer/obj inspectors for the incoming data source
    AbstractSerDe serDe = ReflectionUtils.newInstance(runtimeValuesInfo.getTableDesc().getSerDeClass(), null);
    serDe.initialize(rct.conf, runtimeValuesInfo.getTableDesc().getProperties(), null);
    ObjectInspector inspector = serDe.getObjectInspector();

    // Set up col expressions for the dynamic values using this input
    List<ExprNodeEvaluator> colExprEvaluators = new ArrayList<ExprNodeEvaluator>();
    for (ExprNodeDesc expr : runtimeValuesInfo.getColExprs()) {
      ExprNodeEvaluator exprEval = ExprNodeEvaluatorFactory.get(expr, null);
      exprEval.initialize(inspector);
      colExprEvaluators.add(exprEval);
    }

    KeyValueReader kvReader = (KeyValueReader) runtimeValueInput.getReader();
    long rowCount = 0;
    while (kvReader.next()) {
      Object row = serDe.deserialize((Writable) kvReader.getCurrentValue());
      rowCount++;
      for (int colIdx = 0; colIdx < colExprEvaluators.size(); ++colIdx) {
        // Read each expression and save it to the value registry
        ExprNodeEvaluator eval = colExprEvaluators.get(colIdx);
        Object val = eval.evaluate(row);
        setValue(runtimeValuesInfo.getDynamicValueIDs().get(colIdx), val);
      }
    }
    // For now, expecting a single row (min/max, aggregated bloom filter), or no rows
    if (rowCount == 0) {
      LOG.debug("No input rows from " + inputSourceName + ", filling dynamic values with nulls");
      for (int colIdx = 0; colIdx < colExprEvaluators.size(); ++colIdx) {
        ExprNodeEvaluator eval = colExprEvaluators.get(colIdx);
        setValue(runtimeValuesInfo.getDynamicValueIDs().get(colIdx), null);
      }
    } else if (rowCount > 1) {
      throw new IllegalStateException("Expected 0 or 1 rows from " + inputSourceName + ", got " + rowCount);
    }
  }
}
