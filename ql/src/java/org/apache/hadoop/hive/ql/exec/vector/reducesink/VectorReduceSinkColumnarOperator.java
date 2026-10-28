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

package org.apache.hadoop.hive.ql.exec.vector.reducesink;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.keyseries.VectorKeySeriesBytesSerialized;
import org.apache.hadoop.hive.ql.exec.vector.keyseries.VectorKeySeriesLongSerialized;
import org.apache.hadoop.hive.ql.exec.vector.keyseries.VectorKeySeriesMultiSerialized;
import org.apache.hadoop.hive.ql.exec.vector.shuffle.ColumnarShuffleWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * A ReduceSink of an unsorted edge that ships its rows in columnar blocks (ColumnarShuffleWriter): the rows of a
 * partition gather in column vectors and leave as one record per block, and a batch whose rows all go to one partition
 * (a repeating key) leaves as a block of its own, so neither a row's value serialization nor a record per row is paid. A
 * row's partition is the one the row path gives it: the row path's own key series hashes the BinarySortable key of each
 * run of equal keys (Murmur3), so the other inputs of the reducer may be row or columnar. Rows are hashed modulo the
 * partitions of the edge at run time, as the partitioner takes the row path's hash codes, and a block record's hash
 * code is its partition, which the partitioner returns.
 */
public class VectorReduceSinkColumnarOperator extends VectorReduceSinkUniformHashOperator {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorReduceSinkColumnarOperator.class.getName();

  private transient ColumnarShuffleWriter writer;
  private transient int[] partitionOfRow;

  /** Kryo ctor. */
  protected VectorReduceSinkColumnarOperator() {
    super();
  }

  public VectorReduceSinkColumnarOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorReduceSinkColumnarOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    super(ctx, conf, vContext, vectorDesc);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    // the key series of the row path's ReduceSink of this key shape (VectorReduceSinkLongOperator, StringOperator,
    // MultiKeyOperator): the BinarySortable key and its hash, a run of equal keys at once
    serializedKeySeries = switch (vectorDesc.reduceSinkKeyType()) {
      case LONG -> new VectorKeySeriesLongSerialized<>(reduceSinkKeyColumnMap[0],
          (PrimitiveTypeInfo) reduceSinkKeyTypeInfos[0], keyBinarySortableSerializeWrite);
      case STRING -> new VectorKeySeriesBytesSerialized<>(reduceSinkKeyColumnMap[0], keyBinarySortableSerializeWrite);
      default -> {
        VectorKeySeriesMultiSerialized<BinarySortableSerializeWrite> multiKeySeries =
            new VectorKeySeriesMultiSerialized<>(keyBinarySortableSerializeWrite);
        multiKeySeries.init(reduceSinkKeyTypeInfos, reduceSinkKeyColumnMap);
        yield multiKeySeries;
      }
    };
    partitionOfRow = new int[VectorizedRowBatch.DEFAULT_SIZE];
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) row;
    batchCounter++;
    if (batch.size == 0) {
      return;
    }
    if (reduceSinkKeyExpressions != null) {
      for (VectorExpression ve : reduceSinkKeyExpressions) {
        ve.evaluate(batch);
      }
    }
    if (reduceSinkValueExpressions != null) {
      for (VectorExpression ve : reduceSinkValueExpressions) {
        ve.evaluate(batch);
      }
    }
    try {
      if (writer == null) {
        // the column types are known with the first batch: the key columns, then the value columns (none for a
        // ReduceSink of keys only, whose value column map is not set)
        int[] valueMap = isEmptyValue ? new int[0] : reduceSinkValueColumnMap;
        int[] columnMap = new int[reduceSinkKeyColumnMap.length + valueMap.length];
        System.arraycopy(reduceSinkKeyColumnMap, 0, columnMap, 0, reduceSinkKeyColumnMap.length);
        System.arraycopy(valueMap, 0, columnMap, reduceSinkKeyColumnMap.length, valueMap.length);
        writer = new ColumnarShuffleWriter(batch, columnMap, outputPartitions(), this::collectBlock);
      }
      int capacity = batch.cols[reduceSinkKeyColumnMap[0]].isNull.length;
      if (partitionOfRow.length < capacity) {
        partitionOfRow = new int[capacity];
      }
      int[] selected = batch.selectedInUse ? batch.selected : null;
      serializedKeySeries.processBatch(batch);
      do {
        int hash = serializedKeySeries.getCurrentIsAllNull()
            ? nullKeyHashCode : serializedKeySeries.getCurrentHashCode();
        int partition = writer.partitionOf(hash);
        int logical = serializedKeySeries.getCurrentLogical();
        int end = logical + serializedKeySeries.getCurrentDuplicateCount();
        for (; logical < end; logical++) {
          partitionOfRow[selected == null ? logical : selected[logical]] = partition;
        }
      } while (serializedKeySeries.next());
      writer.write(batch, partitionOfRow);
      numRows += batch.size;
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    if (!abort && writer != null) {
      try {
        writer.close();
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }
    super.closeOp(abort);
  }
}
