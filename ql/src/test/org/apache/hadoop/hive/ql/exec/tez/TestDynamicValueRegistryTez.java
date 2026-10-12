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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

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
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.junit.Test;

public class TestDynamicValueRegistryTez {
  private static final String SOURCE = "Reducer 2";
  private static final String VALUE_ID = "RS_7_col0";

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

  private RegistryConfTez conf(int dagIdentifier, LogicalInput input) {
    ProcessorContext context = mock(ProcessorContext.class);
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
