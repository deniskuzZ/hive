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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.NoDynamicValuesException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.tez.DynamicValueRegistryTez.RegistryConfTez;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
import org.apache.hadoop.hive.ql.plan.DynamicValue;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.junit.Test;
import org.mockito.Mockito;

public class TestDynamicValueRegistryTez {
  private static final String SOURCE = "Reducer 2";
  private static final String VALUE_ID = "RS_7_col0";
  private static final ApplicationId APPLICATION = ApplicationId.newInstance(1L, 1);
  private static final String QUERY_ID = "hive_query_retried";

  private final Configuration conf = new Configuration();
  private final MapWork work = new MapWork("Map 1");

  public TestDynamicValueRegistryTez() {
    Properties properties = new Properties();
    properties.setProperty(serdeConstants.SERIALIZATION_LIB, LazyBinarySerDe.class.getName());
    properties.setProperty(serdeConstants.LIST_COLUMNS, "_col0");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "bigint");
    RuntimeValuesInfo info = new RuntimeValuesInfo();
    info.setTableDesc(new TableDesc(SequenceFileInputFormat.class, SequenceFileOutputFormat.class, properties));
    info.setDynamicValueIDs(List.of(VALUE_ID));
    info.setColExprs(List.of(new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "_col0", "", false)));
    work.setInputSourceToRuntimeValuesInfo(SOURCE, info);
  }

  @Test
  public void testTasksOfAVertexReadASourceOnce() throws Exception {
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    LogicalInput first = input(42L);
    LogicalInput second = input(42L);

    registry.init(conf(1, first));
    registry.init(conf(1, second));

    verify(first, times(1)).start();
    verify(second, times(0)).start();
    assertEquals(new LongWritable(42), registry.getValue(VALUE_ID));
  }

  /** A source whose read failed is not marked read: the next task of the vertex reads it. */
  @Test
  public void testFailedReadIsRetriedByTheNextTask() throws Exception {
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    LogicalInput failing = input(42L);
    when(failing.getReader()).thenThrow(new IOException("lost the source"));
    LogicalInput next = input(42L);

    assertThrows(IOException.class, () -> registry.init(conf(1, failing)));
    registry.init(conf(1, next));

    verify(next, times(1)).start();
    assertEquals(new LongWritable(42), registry.getValue(VALUE_ID));
  }

  /** Tasks of a vertex starting together read a source once, and every one of them sees its values. */
  @Test
  public void testConcurrentTasksReadASourceOnce() throws Exception {
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    int tasks = 8;
    List<LogicalInput> inputs = new ArrayList<>();
    for (int i = 0; i < tasks; i++) {
      inputs.add(input(42L));
    }
    ExecutorService pool = Executors.newFixedThreadPool(tasks);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> inits = new ArrayList<>();
      for (LogicalInput input : inputs) {
        inits.add(pool.submit(() -> {
          start.await();
          registry.init(conf(1, input));
          return null;
        }));
      }
      start.countDown();
      for (Future<?> init : inits) {
        init.get();
      }
    } finally {
      pool.shutdownNow();
    }
    assertEquals(new LongWritable(42), registry.getValue(VALUE_ID));
    int reads = 0;
    for (LogicalInput input : inputs) {
      reads += Mockito.mockingDetails(input).getInvocations().stream()
          .filter(invocation -> invocation.getMethod().getName().equals("start")).count();
    }
    assertEquals(1, reads);
  }

  /**
   * On LLAP a query re-executed after its AM was lost keeps its query id and its value ids, and the query's cache
   * outlives the lost attempt: the retry's DAG has a registry of its own, so a value stays missing until the retry's
   * source arrives, on its task's thread and on a thread without the task (an LLAP IO thread evaluating a SARG).
   */
  @Test
  public void testARetryDoesNotReadTheValuesOfTheLostAttempt() throws Exception {
    LlapProxy.setDaemon(true);
    ExecutorService task = Executors.newSingleThreadExecutor();
    try {
      Configuration lost = taskConf(APPLICATION);
      ObjectCache cache = ObjectCacheFactory.getCache(lost, QUERY_ID, false, true);
      DynamicValueRegistryTez lostRegistry =
          cache.retrieve(DynamicValue.registryKey(lost), DynamicValueRegistryTez::new);
      lostRegistry.init(conf(APPLICATION, 1, input(2000L)));
      lostRegistry.release();

      ApplicationId retryApplication = ApplicationId.newInstance(2L, 1);
      Configuration retry = taskConf(retryApplication);
      AtomicBoolean ready = new AtomicBoolean();
      RegistryConfTez retryConf = conf(retryApplication, 1, input(2500L), ready);
      task.submit(() -> {
        cache.<DynamicValueRegistryTez>retrieve(DynamicValue.registryKey(retry), DynamicValueRegistryTez::new)
            .init(retryConf);
        return null;
      }).get();

      assertThrows(NoDynamicValuesException.class, () -> value(retry).getValue());
      assertEquals(NoDynamicValuesException.class, task.submit(() -> {
        try {
          return value(retry).getValue();
        } catch (NoDynamicValuesException e) {
          return e.getClass();
        }
      }).get());
      ready.set(true);
      assertEquals(new LongWritable(2500), task.submit(() -> value(retry).getValue()).get());
    } finally {
      task.shutdownNow();
      ObjectCacheFactory.removeLlapQueryCache(QUERY_ID);
      LlapProxy.setDaemon(false);
    }
  }

  /** A value whose filters wait for it is read on its task's input, where a value they pass rows for is missing. */
  @Test
  public void testAValueWaitedForIsReadOnItsTasksInput() throws Exception {
    LlapProxy.setDaemon(true);
    ExecutorService task = Executors.newSingleThreadExecutor();
    try {
      Configuration taskConf = taskConf(APPLICATION);
      ObjectCache cache = ObjectCacheFactory.getCache(taskConf, QUERY_ID, false, true);
      RegistryConfTez registryConf = conf(APPLICATION, 1, input(42L), new AtomicBoolean());
      task.submit(() -> {
        cache.<DynamicValueRegistryTez>retrieve(DynamicValue.registryKey(taskConf), DynamicValueRegistryTez::new)
            .init(registryConf);
        return null;
      }).get();

      assertEquals(NoDynamicValuesException.class, task.submit(() -> {
        try {
          return value(taskConf).getValue();
        } catch (NoDynamicValuesException e) {
          return e.getClass();
        }
      }).get());
      DynamicValue waited = value(taskConf);
      waited.setWaitForArrival(true);
      assertEquals(new LongWritable(42), task.submit(waited::getValue).get());
    } finally {
      task.shutdownNow();
      ObjectCacheFactory.removeLlapQueryCache(QUERY_ID);
      LlapProxy.setDaemon(false);
    }
  }

  /**
   * A task interrupted while it waits for a value (killed) stays interrupted: the exception that reports it may be
   * swallowed (ORC's SargApplier skips a predicate that throws), and the task's next wait must not block.
   */
  @Test
  public void testAnInterruptedWaitKeepsTheInterrupt() throws Exception {
    LlapProxy.setDaemon(true);
    try {
      Configuration taskConf = taskConf(APPLICATION);
      ObjectCache cache = ObjectCacheFactory.getCache(taskConf, QUERY_ID, false, true);
      RegistryConfTez registryConf = conf(APPLICATION, 1, input(42L), new AtomicBoolean());
      CountDownLatch waiting = new CountDownLatch(1);
      doAnswer(invocation -> {
        waiting.countDown();
        new CountDownLatch(1).await();
        return null;
      }).when(registryConf.processorContext).waitForAllInputsReady(anyCollection());
      DynamicValue waited = value(taskConf);
      waited.setWaitForArrival(true);
      AtomicReference<Object> interrupted = new AtomicReference<>();
      Thread task = new Thread(() -> {
        try {
          cache.<DynamicValueRegistryTez>retrieve(DynamicValue.registryKey(taskConf), DynamicValueRegistryTez::new)
              .init(registryConf);
          waited.getValue();
          interrupted.set("no exception");
        } catch (IllegalStateException e) {
          interrupted.set(Thread.currentThread().isInterrupted());
        } catch (Exception e) {
          interrupted.set(e);
        }
      });
      task.start();
      assertTrue(waiting.await(1, TimeUnit.MINUTES));
      task.interrupt();
      task.join(TimeUnit.MINUTES.toMillis(1));

      assertEquals(true, interrupted.get());
    } finally {
      ObjectCacheFactory.removeLlapQueryCache(QUERY_ID);
      LlapProxy.setDaemon(false);
    }
  }

  /** With hive.tez.dynamic.semijoin.reduction.wait.time, a task waits for its own input at its start. */
  @Test
  public void testInitWaitsForItsOwnInputWithinTheWaitTime() throws Exception {
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TEZ_DYNAMIC_SEMIJOIN_REDUCTION_WAIT_TIME, 10, TimeUnit.SECONDS);
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    RegistryConfTez registryConf = conf(1, input(42L), new AtomicBoolean());
    // the input is ready within the time it is given
    when(registryConf.processorContext.waitForAllInputsReady(any(), anyLong()))
        .thenAnswer(invocation -> (long) invocation.getArgument(1) > 0);
    ExecutorService task = Executors.newSingleThreadExecutor();
    try {
      task.submit(() -> {
        registry.init(registryConf);
        return null;
      }).get();
      assertEquals(new LongWritable(42), task.submit(() -> registry.getValue(VALUE_ID)).get());
      verify(registryConf.processorContext).waitForAllInputsReady(any(), longThat(t -> t > 0 && t <= 10_000));
    } finally {
      task.shutdownNow();
    }
  }

  /** Within the wait time, a task waits for the task that fetches its source instead of starting its own input. */
  @Test
  public void testATaskWaitsForTheTaskFetchingItsSource() throws Exception {
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TEZ_DYNAMIC_SEMIJOIN_REDUCTION_WAIT_TIME, 1, TimeUnit.MINUTES);
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    AtomicBoolean firstReady = new AtomicBoolean();
    RegistryConfTez first = conf(1, input(42L), firstReady);
    LogicalInput secondInput = input(42L);
    RegistryConfTez second = conf(1, secondInput, new AtomicBoolean(true));
    ExecutorService firstTask = Executors.newSingleThreadExecutor();
    try {
      firstTask.submit(() -> {
        registry.init(first);
        return null;
      }).get();
      CompletableFuture<Object> secondValue = new CompletableFuture<>();
      Thread secondTask = new Thread(() -> {
        try {
          registry.init(second);
          secondValue.complete(registry.getValue(VALUE_ID));
        } catch (Exception e) {
          secondValue.completeExceptionally(e);
        }
      });
      secondTask.start();
      awaitWaiting(secondTask);
      assertFalse(secondValue.isDone());

      firstReady.set(true);
      assertEquals(new LongWritable(42), firstTask.submit(() -> registry.getValue(VALUE_ID)).get());
      assertEquals(new LongWritable(42), secondValue.get(1, TimeUnit.MINUTES));
      verify(secondInput, never()).start();
    } finally {
      firstTask.shutdownNow();
    }
  }

  /** A task waiting for the task that fetches its source takes the source over when that task ends first. */
  @Test
  public void testATaskTakesOverASourceWhoseReaderEnded() throws Exception {
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TEZ_DYNAMIC_SEMIJOIN_REDUCTION_WAIT_TIME, 1, TimeUnit.MINUTES);
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    RegistryConfTez first = conf(1, input(42L), new AtomicBoolean());
    LogicalInput secondInput = input(42L);
    RegistryConfTez second = conf(1, secondInput, new AtomicBoolean(true));
    ExecutorService firstTask = Executors.newSingleThreadExecutor();
    try {
      firstTask.submit(() -> {
        registry.init(first);
        return null;
      }).get();
      CompletableFuture<Object> secondValue = new CompletableFuture<>();
      Thread secondTask = new Thread(() -> {
        try {
          registry.init(second);
          secondValue.complete(registry.getValue(VALUE_ID));
        } catch (Exception e) {
          secondValue.completeExceptionally(e);
        }
      });
      secondTask.start();
      awaitWaiting(secondTask);

      firstTask.submit(registry::release).get();
      assertEquals(new LongWritable(42), secondValue.get(1, TimeUnit.MINUTES));
      verify(secondInput, times(1)).start();
    } finally {
      firstTask.shutdownNow();
    }
  }

  private static void awaitWaiting(Thread thread) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);
    while (thread.getState() != Thread.State.TIMED_WAITING && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(Thread.State.TIMED_WAITING, thread.getState());
  }

  /** A task does not wait for a runtime value source: the value is missing until its input is ready. */
  @Test
  public void testInitDoesNotWaitForASource() throws Exception {
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    LogicalInput input = input(42L);
    AtomicBoolean ready = new AtomicBoolean();
    RegistryConfTez conf = conf(1, input, ready);
    ExecutorService task = Executors.newSingleThreadExecutor();
    try {
      task.submit(() -> {
        registry.init(conf);
        return null;
      }).get();
      verify(input, times(1)).start();
      assertMissing(task, registry);

      ready.set(true);
      assertEquals(new LongWritable(42), task.submit(() -> registry.getValue(VALUE_ID)).get());
      verify(conf.processorContext, never()).waitForAllInputsReady(anyCollection());
    } finally {
      task.shutdownNow();
    }
  }

  /** While one task fetches a source, the other tasks of the vertex do not start their own input for it. */
  @Test
  public void testOneTaskFetchesASource() throws Exception {
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    LogicalInput first = input(42L);
    LogicalInput second = input(42L);
    AtomicBoolean ready = new AtomicBoolean();
    ExecutorService firstTask = Executors.newSingleThreadExecutor();
    ExecutorService secondTask = Executors.newSingleThreadExecutor();
    try {
      firstTask.submit(() -> {
        registry.init(conf(1, first, ready));
        return null;
      }).get();
      secondTask.submit(() -> {
        registry.init(conf(1, second, ready));
        return null;
      }).get();
      assertMissing(secondTask, registry);

      ready.set(true);
      assertMissing(secondTask, registry);
      assertEquals(new LongWritable(42), firstTask.submit(() -> registry.getValue(VALUE_ID)).get());
      assertEquals(new LongWritable(42), secondTask.submit(() -> registry.getValue(VALUE_ID)).get());
      verify(second, never()).start();
    } finally {
      firstTask.shutdownNow();
      secondTask.shutdownNow();
    }
  }

  /** A task that ends before its source arrives hands the source to the next task that needs the value. */
  @Test
  public void testAReleasedSourceGoesToTheNextTask() throws Exception {
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    LogicalInput first = input(42L);
    LogicalInput second = input(42L);
    AtomicBoolean ready = new AtomicBoolean();
    ExecutorService firstTask = Executors.newSingleThreadExecutor();
    ExecutorService secondTask = Executors.newSingleThreadExecutor();
    try {
      firstTask.submit(() -> {
        registry.init(conf(1, first, ready));
        registry.release();
        return null;
      }).get();
      secondTask.submit(() -> {
        registry.init(conf(1, second, ready));
        return null;
      }).get();

      ready.set(true);
      assertEquals(new LongWritable(42), secondTask.submit(() -> registry.getValue(VALUE_ID)).get());
      verify(second, times(1)).start();
    } finally {
      firstTask.shutdownNow();
      secondTask.shutdownNow();
    }
  }

  /** A consumer that cannot pass rows waits for the source, on its own task's input. */
  @Test
  public void testWaitForValueWaitsOnItsOwnInput() throws Exception {
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    LogicalInput first = input(42L);
    LogicalInput second = input(42L);
    RegistryConfTez secondConf = conf(1, second, new AtomicBoolean());
    ExecutorService firstTask = Executors.newSingleThreadExecutor();
    ExecutorService secondTask = Executors.newSingleThreadExecutor();
    try {
      firstTask.submit(() -> {
        registry.init(conf(1, first, new AtomicBoolean()));
        return null;
      }).get();
      Object value = secondTask.submit(() -> {
        registry.init(secondConf);
        return registry.waitForValue(VALUE_ID);
      }).get();

      assertEquals(new LongWritable(42), value);
      verify(second, times(1)).start();
      verify(secondConf.processorContext, times(1)).waitForAllInputsReady(anyCollection());
    } finally {
      firstTask.shutdownNow();
      secondTask.shutdownNow();
    }
  }

  private static void assertMissing(ExecutorService task, DynamicValueRegistryTez registry) throws Exception {
    assertEquals(NoDynamicValuesException.class, task.submit(() -> {
      try {
        registry.getValue(VALUE_ID);
        return null;
      } catch (NoDynamicValuesException e) {
        return e.getClass();
      }
    }).get());
  }

  /** The conf of a task of the first DAG of the query in the application. */
  private static Configuration taskConf(ApplicationId application) {
    Configuration conf = new Configuration();
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, QUERY_ID);
    conf.set(MRInput.TEZ_MAPREDUCE_DAG_ID, TezDAGID.getInstance(application, 1).toString());
    return conf;
  }

  private static DynamicValue value(Configuration conf) {
    DynamicValue value = new DynamicValue(VALUE_ID, TypeInfoFactory.longTypeInfo);
    value.setConf(conf);
    return value;
  }

  private RegistryConfTez conf(int dagIdentifier, LogicalInput input) throws Exception {
    return conf(APPLICATION, dagIdentifier, input);
  }

  private RegistryConfTez conf(int dagIdentifier, LogicalInput input, AtomicBoolean ready) throws Exception {
    return conf(APPLICATION, dagIdentifier, input, ready);
  }

  private RegistryConfTez conf(ApplicationId application, int dagIdentifier, LogicalInput input) throws Exception {
    return conf(application, dagIdentifier, input, new AtomicBoolean(true));
  }

  /** A task's registry configuration whose input is ready once {@code ready} is set. */
  private RegistryConfTez conf(ApplicationId application, int dagIdentifier, LogicalInput input, AtomicBoolean ready)
      throws Exception {
    ProcessorContext context = mock(ProcessorContext.class);
    when(context.getApplicationId()).thenReturn(application);
    when(context.getDagIdentifier()).thenReturn(dagIdentifier);
    when(context.waitForAllInputsReady(any(), anyLong())).thenAnswer(invocation -> ready.get());
    return new RegistryConfTez(conf, work, context, Collections.singletonMap(SOURCE, input));
  }

  private static LogicalInput input(long value) throws Exception {
    LazyBinarySerDe serDe = new LazyBinarySerDe();
    Properties properties = new Properties();
    properties.setProperty(serdeConstants.LIST_COLUMNS, "_col0");
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "bigint");
    serDe.initialize(new Configuration(), properties, null);
    StructObjectInspector inspector = ObjectInspectorFactory.getStandardStructObjectInspector(List.of("_col0"),
        List.of(PrimitiveObjectInspectorFactory.javaLongObjectInspector));
    Writable row = serDe.serialize(List.of(value), inspector);

    KeyValueReader reader = mock(KeyValueReader.class);
    when(reader.next()).thenReturn(true, false);
    when(reader.getCurrentValue()).thenReturn(row);
    LogicalInput input = mock(LogicalInput.class);
    when(input.getReader()).thenReturn(reader);
    return input;
  }
}
