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
package org.apache.hadoop.hive.ql.exec;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ForwardDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

public class TestOperatorDeferredAsyncInit {

  /**
   * Two map joins, each initialized by its own dummy parent, start loading their hash tables before
   * either is waited for: each load here finishes only once both have started.
   */
  @Test
  public void testAsyncInitializationsOverlap() throws Exception {
    ExecutorService loaders = Executors.newCachedThreadPool();
    try {
      CompilationOpContext context = new CompilationOpContext();
      CountDownLatch started = new CountDownLatch(2);
      ForwardOperator dummy1 = new ForwardOperator(context);
      ForwardOperator dummy2 = new ForwardOperator(context);
      AsyncInitOperator join1 = new AsyncInitOperator(context, loaders, started);
      AsyncInitOperator join2 = new AsyncInitOperator(context, loaders, started);
      dummy1.setChildOperators(List.of(join1));
      join1.setParentOperators(List.of(dummy1));
      dummy2.setChildOperators(List.of(join2));
      join2.setParentOperators(List.of(dummy2));
      dummy1.setConf(new ForwardDesc());
      dummy2.setConf(new ForwardDesc());
      join1.setConf(new ForwardDesc());
      join2.setConf(new ForwardDesc());
      ObjectInspector[] inspectors = {PrimitiveObjectInspectorFactory.javaLongObjectInspector};
      Configuration conf = new Configuration();

      Operator.initializeDeferringAsyncWaits(() -> {
        dummy1.initialize(conf, inspectors);
        dummy2.initialize(conf, inspectors);
      });

      assertTrue("join 1 loaded while join 2 loaded", join1.overlapped);
      assertTrue("join 2 loaded while join 1 loaded", join2.overlapped);
    } finally {
      loaders.shutdownNow();
    }
  }

  /** An operator whose async initialization finishes once every operator sharing the latch has started its own. */
  private static final class AsyncInitOperator extends Operator<ForwardDesc> {
    private final transient ExecutorService loaders;
    private final transient CountDownLatch started;
    private boolean overlapped;

    AsyncInitOperator(CompilationOpContext context, ExecutorService loaders, CountDownLatch started) {
      super(context);
      this.loaders = loaders;
      this.started = started;
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
      super.initializeOp(hconf);
      asyncInitOperations.add(loaders.submit(() -> {
        started.countDown();
        return started.await(5, TimeUnit.SECONDS);
      }));
    }

    @Override
    protected void completeInitializationOp(Object[] os) {
      overlapped = (Boolean) os[0];
    }

    @Override
    public void process(Object row, int tag) {
    }

    @Override
    public String getName() {
      return "ASYNC";
    }

    @Override
    public OperatorType getType() {
      return null;
    }
  }
}
