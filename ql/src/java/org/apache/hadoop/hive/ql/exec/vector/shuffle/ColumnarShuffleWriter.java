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

package org.apache.hadoop.hive.ql.exec.vector.shuffle;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Writes the rows of a columnar shuffle's ReduceSink as blocks. A batch whose rows all go to one partition travels as
 * one block of its own, a repeating column as its one value; the rows of the other batches gather per partition in
 * ColumnarPartitionBuffers. The edge is unsorted, so a partition's rows may arrive in any order.
 */
public final class ColumnarShuffleWriter {

  /** Receives an encoded block: its bytes are the buffer's [0, position). */
  public interface BlockCollector {
    void collect(int partition, ByteBuffer block) throws IOException;
  }

  /** The memory the vectors of all partitions take, as Trino's partitioner buffers (32 MB per driver). */
  static final long BUFFER_BYTES = 32L << 20;
  /** The fewest rows of a block: a batch of fewer rows to one partition gathers with the other rows. */
  static final int MIN_BLOCK_ROWS = 64;

  private final int partitions;
  private final int[] columnMap;
  private final ColumnarPartitionBuffers buffers;
  private final BlockCollector collector;
  private final ColumnVector[] batchColumns;
  private int[] partitionOfRow = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private ByteBuffer block = ByteBuffer.allocate(0);

  /**
   * @param batch a batch of the source layout, for the column types
   * @param columnMap the source columns that travel, in their order in a block
   */
  public ColumnarShuffleWriter(VectorizedRowBatch batch, int[] columnMap, int partitions, BlockCollector collector) {
    this.partitions = partitions;
    this.columnMap = columnMap;
    this.collector = collector;
    this.batchColumns = new ColumnVector[columnMap.length];
    this.buffers = new ColumnarPartitionBuffers(batch, columnMap, partitions, blockRows(batch, columnMap, partitions),
        (partition, columns, rows) -> emit(partition, columns, null, rows));
  }

  /** The partition of a row path hash code, as the partitioner computes it. */
  public int partitionOf(int hashCode) {
    return (hashCode & Integer.MAX_VALUE) % partitions;
  }

  /** Writes the rows of the batch (honoring its selection) to the partitions given per batch row index. */
  public void write(VectorizedRowBatch batch, int[] partition) throws IOException {
    int[] selected = batch.selectedInUse ? batch.selected : null;
    int first = partition[selected == null ? 0 : selected[0]];
    for (int k = 1; k < batch.size; k++) {
      if (partition[selected == null ? k : selected[k]] != first) {
        buffers.append(batch, partition);
        return;
      }
    }
    write(batch, first);
  }

  /** Writes every row of the batch (honoring its selection) to one partition. */
  public void write(VectorizedRowBatch batch, int partition) throws IOException {
    if (batch.size >= MIN_BLOCK_ROWS) {
      for (int c = 0; c < columnMap.length; c++) {
        batchColumns[c] = batch.cols[columnMap[c]];
      }
      emit(partition, batchColumns, batch.selectedInUse ? batch.selected : null, batch.size);
      return;
    }
    int capacity = batch.cols[columnMap[0]].isNull.length;
    if (partitionOfRow.length < capacity) {
      partitionOfRow = new int[capacity];
    }
    for (int k = 0; k < batch.size; k++) {
      partitionOfRow[batch.selectedInUse ? batch.selected[k] : k] = partition;
    }
    buffers.append(batch, partitionOfRow);
  }

  /** Emits the rows still gathered. */
  public void close() throws IOException {
    buffers.flushAll();
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

  private void emit(int partition, ColumnVector[] columns, int[] selected, int rows) throws IOException {
    int size = ColumnarShuffleBlock.encodedSize(columns, selected, rows);
    if (block.capacity() < size) {
      block = ByteBuffer.allocate(Math.max(size, 2 * block.capacity()));
    }
    block.clear();
    ColumnarShuffleBlock.encode(columns, selected, rows, block);
    collector.collect(partition, block);
  }
}
