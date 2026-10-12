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
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.tez.DynamicValueRegistryTez.RegistryConfTez;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
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
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.junit.Test;
import org.mockito.Mockito;

public class TestDynamicValueRegistryTez {
  private static final String SOURCE = "Reducer 2";
  private static final String VALUE_ID = "RS_7_col0";
  private static final ApplicationId APPLICATION = ApplicationId.newInstance(1L, 1);

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

  @Test
  public void testEachDagReadsASource() throws Exception {
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    LogicalInput first = input(42L);
    LogicalInput second = input(7L);

    registry.init(conf(1, first));
    registry.init(conf(2, second));

    verify(second, times(1)).start();
    assertEquals(new LongWritable(7), registry.getValue(VALUE_ID));
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
      List<Future<Object>> values = new ArrayList<>();
      for (LogicalInput input : inputs) {
        values.add(pool.submit(() -> {
          start.await();
          registry.init(conf(1, input));
          return registry.getValue(VALUE_ID);
        }));
      }
      start.countDown();
      for (Future<Object> value : values) {
        assertEquals(new LongWritable(42), value.get());
      }
    } finally {
      pool.shutdownNow();
    }
    int reads = 0;
    for (LogicalInput input : inputs) {
      reads += Mockito.mockingDetails(input).getInvocations().stream()
          .filter(invocation -> invocation.getMethod().getName().equals("start")).count();
    }
    assertEquals(1, reads);
  }

  /** A query re-executed after its AM was lost runs its DAGs, numbered from 1 again, in another application. */
  @Test
  public void testADagOfAnotherApplicationReadsASource() throws Exception {
    DynamicValueRegistryTez registry = new DynamicValueRegistryTez();
    LogicalInput first = input(42L);
    LogicalInput second = input(7L);

    registry.init(conf(1, first));
    registry.init(conf(ApplicationId.newInstance(2L, 1), 1, second));

    verify(second, times(1)).start();
    assertEquals(new LongWritable(7), registry.getValue(VALUE_ID));
  }

  private RegistryConfTez conf(int dagIdentifier, LogicalInput input) {
    return conf(APPLICATION, dagIdentifier, input);
  }

  private RegistryConfTez conf(ApplicationId application, int dagIdentifier, LogicalInput input) {
    ProcessorContext context = mock(ProcessorContext.class);
    when(context.getApplicationId()).thenReturn(application);
    when(context.getDagIdentifier()).thenReturn(dagIdentifier);
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
