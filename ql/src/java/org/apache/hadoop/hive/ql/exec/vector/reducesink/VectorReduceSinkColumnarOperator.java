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
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.PartitionedOutputCollector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.keyseries.VectorKeySeriesBytesSerialized;
import org.apache.hadoop.hive.ql.exec.vector.keyseries.VectorKeySeriesLongSerialized;
import org.apache.hadoop.hive.ql.exec.vector.keyseries.VectorKeySeriesMultiSerialized;
import org.apache.hadoop.hive.ql.exec.vector.shuffle.ColumnarPartitionBuffers;
import org.apache.hadoop.hive.ql.exec.vector.shuffle.ColumnarShuffleBlock;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * A ReduceSink of an unsorted edge that ships its rows in columnar blocks: the rows of a partition gather in column
 * vectors and leave as one record per block, so neither a row's value serialization nor a record per row is paid. A
 * row's partition is the one the row path gives it: the row path's own key series hashes the BinarySortable key of each
 * run of equal keys (Murmur3), so the other inputs of the reducer may be row or columnar. Rows are hashed modulo the
 * partitions of the edge at run time, as the partitioner takes the row path's hash codes, and a block record's hash
 * code is its partition, which the partitioner returns.
 */
public class VectorReduceSinkColumnarOperator extends VectorReduceSinkUniformHashOperator {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorReduceSinkColumnarOperator.class.getName();

  /** The memory the vectors of all partitions take, as Trino's partitioner buffers (32 MB per driver). */
  static final long BUFFER_BYTES = 32L << 20;
  static final int MIN_BLOCK_ROWS = 64;

  private transient int partitions;
  private transient ColumnarPartitionBuffers buffers;
  private transient int[] partitionOfRow;
  private transient ByteBuffer block;

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
    block = ByteBuffer.allocate(0);
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
      if (buffers == null) {
        // the edge's partitions: the output collector is set after the operators are initialized (without one,
        // nothing is written)
        partitions = out instanceof PartitionedOutputCollector partitioned ? partitioned.getNumPartitions() : 1;
        // the column types are known with the first batch: the key columns, then the value columns (none for a
        // ReduceSink of keys only, whose value column map is not set)
        int[] valueMap = isEmptyValue ? new int[0] : reduceSinkValueColumnMap;
        int[] columnMap = new int[reduceSinkKeyColumnMap.length + valueMap.length];
        System.arraycopy(reduceSinkKeyColumnMap, 0, columnMap, 0, reduceSinkKeyColumnMap.length);
        System.arraycopy(valueMap, 0, columnMap, reduceSinkKeyColumnMap.length, valueMap.length);
        buffers = new ColumnarPartitionBuffers(batch, columnMap, partitions,
            blockRows(batch, columnMap, partitions), this::emit);
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
        int partition = (hash & Integer.MAX_VALUE) % partitions;
        int logical = serializedKeySeries.getCurrentLogical();
        int end = logical + serializedKeySeries.getCurrentDuplicateCount();
        for (; logical < end; logical++) {
          partitionOfRow[selected == null ? logical : selected[logical]] = partition;
        }
      } while (serializedKeySeries.next());
      buffers.append(batch, partitionOfRow);
      numRows += batch.size;
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  /** The rows of a block: the vectors of all partitions fit the buffer, a block holds a batch at most. */
  static int blockRows(VectorizedRowBatch batch, int[] columnMap, int partitions) {
    long rowBytes = 0;
    for (int column : columnMap) {
      // a null flag, then the value: a decimal is a HiveDecimalWritable, bytes an estimate of the data
      rowBytes += 1 + switch (batch.cols[column].type) {
        case TIMESTAMP -> Long.BYTES + Integer.BYTES;
        case BYTES -> 2 * Integer.BYTES + Long.BYTES + 32;
        case DECIMAL -> 64;
        default -> Long.BYTES;
      };
    }
    long rows = BUFFER_BYTES / (partitions * rowBytes);
    return (int) Math.max(MIN_BLOCK_ROWS, Math.min(VectorizedRowBatch.DEFAULT_SIZE, rows));
  }

  private void emit(int partition, ColumnVector[] columns, int rows) throws IOException {
    int size = ColumnarShuffleBlock.encodedSize(columns, rows);
    if (block.capacity() < size) {
      block = ByteBuffer.allocate(Math.max(size, 2 * block.capacity()));
    }
    block.clear();
    ColumnarShuffleBlock.encode(columns, rows, block);
    keyWritable.setSize(Integer.BYTES);
    byte[] key = keyWritable.get();
    key[0] = (byte) (partition >>> 24);
    key[1] = (byte) (partition >>> 16);
    key[2] = (byte) (partition >>> 8);
    key[3] = (byte) partition;
    keyWritable.setDistKeyLength(Integer.BYTES);
    keyWritable.setHashCode(partition);
    valueBytesWritable.set(block.array(), 0, block.position());
    if (out != null) {
      out.collect(keyWritable, valueBytesWritable);
    }
  }

  @Override
  protected void closeOp(boolean abort) throws HiveException {
    if (!abort && buffers != null) {
      try {
        buffers.flushAll();
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }
    super.closeOp(abort);
  }
}
