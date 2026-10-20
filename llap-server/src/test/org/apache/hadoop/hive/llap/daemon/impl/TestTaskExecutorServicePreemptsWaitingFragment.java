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
package org.apache.hadoop.hive.llap.daemon.impl;

import static org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorTestHelpers.createQueryInfo;
import static org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorTestHelpers.createSubmitWorkRequestProto;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorTestHelpers.MockRequest;
import org.apache.hadoop.hive.llap.daemon.impl.comparator.ShortestJobFirstComparator;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.EntityDescriptorProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.IOSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.VertexOrBinary;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.junit.Test;

/**
 * Every executor runs a fragment of "Map 2" that waits for the broadcast of "Reducer 3", and the last
 * "Reducer 3" fragment is queued behind them: the queued fragment must preempt the one waiting for it. The
 * finishable-state tracking is real (QueryInfo, QueryFragmentInfo); only the task body is mocked.
 */
public class TestTaskExecutorServicePreemptsWaitingFragment {

  private static final String MAP_2 = "Map 2";
  private static final String REDUCER_3 = "Reducer 3";
  private static final String REDUCER_4 = "Reducer 4";

  /** The source state update of Reducer 4 reaches the daemon: Reducer 3 is finishable and preempts Map 2. */
  @Test(timeout = 30000)
  public void testUpstreamPreemptsWhenSourceStateArrives() throws Exception {
    runUpstreamBehindDownstream(Delivery.STATE_AND_NOTIFICATION);
  }

  /** No source state update reaches the daemon: Map 2 still waits for Reducer 3, so Reducer 3 preempts it. */
  @Test(timeout = 30000)
  public void testUpstreamPreemptsWithoutSourceState() throws Exception {
    runUpstreamBehindDownstream(Delivery.NONE);
  }

  /** The source state is recorded but the queued fragment is not notified. */
  @Test(timeout = 30000)
  public void testUpstreamPreemptsWhenNotificationIsLost() throws Exception {
    runUpstreamBehindDownstream(Delivery.STATE_ONLY);
  }

  private enum Delivery { STATE_AND_NOTIFICATION, STATE_ONLY, NONE }

  private void runUpstreamBehindDownstream(Delivery delivery) throws Exception {
    QueryInfo queryInfo = createQueryInfo();
    // Map 2 <- Reducer 3 (broadcast); Reducer 3 <- Reducer 4. Lower withinDagPriority runs first.
    SourceAwareRequest map2 = request(queryInfo, MAP_2, 2, 15, 1, REDUCER_3, 600_000L);
    SourceAwareRequest reducer3 = request(queryInfo, REDUCER_3, 3, 12, 2, REDUCER_4, 100L);

    TaskExecutorService executor = new TaskExecutorService(1, 2,
        ShortestJobFirstComparator.class.getName(), true,
        Thread.currentThread().getContextClassLoader(), mock(LlapDaemonExecutorMetrics.class), null);
    try {
      executor.schedule(map2);
      map2.awaitStart();
      assertFalse("Map 2 waits for Reducer 3", map2.canFinish());

      executor.schedule(reducer3);
      assertFalse("Reducer 3 is queued while Reducer 4 runs", reducer3.hasStarted());

      if (delivery != Delivery.NONE) {
        queryInfo.getSourceStateMap().put(REDUCER_4, SourceStateProto.S_SUCCEEDED);
      }
      if (delivery == Delivery.STATE_AND_NOTIFICATION) {
        queryInfo.sourceStateUpdated(REDUCER_4);
      }

      long deadline = System.currentTimeMillis() + 5000;
      while (!reducer3.hasStarted() && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
      assertTrue("Map 2 holds the only executor and cannot finish before Reducer 3 runs;"
          + " the queued Reducer 3 must preempt it (delivery=" + delivery + ")", reducer3.hasStarted());
      assertTrue(map2.wasPreempted());
    } finally {
      map2.complete();
      executor.shutDown(false);
    }
  }

  private static SourceAwareRequest request(QueryInfo queryInfo, String vertexName, int vertexIndex,
      int withinDagPriority, int fragmentNumber, String source, long workTime) {
    SubmitWorkRequestProto base = createSubmitWorkRequestProto(fragmentNumber, 10, 0, 100, 200,
        withinDagPriority, "query1", vertexIndex, false);
    SignableVertexSpec vertex = base.getWorkSpec().getVertex().toBuilder()
        .setVertexName(vertexName)
        .addInputSpecs(IOSpecProto.newBuilder()
            .setConnectedVertexName(source)
            .setIoDescriptor(EntityDescriptorProto.newBuilder()
                .setClassName(UnorderedKVInput.class.getName()))
            .setPhysicalEdgeCount(1))
        .build();
    SubmitWorkRequestProto request = base.toBuilder()
        .setWorkSpec(VertexOrBinary.newBuilder().setVertex(vertex)).build();
    QueryFragmentInfo fragmentInfo =
        new QueryFragmentInfo(queryInfo, vertexName, fragmentNumber, 0, vertex, "");
    return new SourceAwareRequest(request, fragmentInfo, workTime);
  }

  /** A MockRequest whose finishable state comes from the real source-state tracking. */
  private static final class SourceAwareRequest extends MockRequest {
    SourceAwareRequest(SubmitWorkRequestProto request, QueryFragmentInfo fragmentInfo, long workTime) {
      super(request, fragmentInfo, false, false, workTime, null, false);
    }

    @Override
    public boolean canFinish() {
      return QueryFragmentInfo.canFinish(getFragmentInfo());
    }

    @Override
    public boolean canFinishForPriority() {
      return getFragmentInfo().canFinishForPriority();
    }

    @Override
    public void updateCanFinishForPriority(boolean value) {
      getFragmentInfo().setCanFinishForPriority(value);
    }
  }
}
