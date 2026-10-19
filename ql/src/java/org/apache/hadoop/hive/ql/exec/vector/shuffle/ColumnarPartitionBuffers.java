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
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * The rows of a columnar shuffle being gathered per output partition: one set of column vectors per partition, filled
 * from the batches of the ReduceSink and handed to the sink as a block when full or at close.
 * <p>
 * A batch is appended in slices of at most a block's rows, each in two passes: the slot of every row in its partition's
 * vectors (a partition whose rows would not fit is emitted first), then one loop per column that copies the values to
 * their slots. The column loops are typed and carry no per-row dispatch; this is the row-wise scatter Trino's
 * PagePartitioner uses when a page holds fewer rows than twice the partitions, as a shuffle to many reducers mostly
 * does.
 */
public final class ColumnarPartitionBuffers {

  /** Receives the full blocks. */
  public interface BlockSink {
    void emit(int partition, ColumnVector[] columns, int rows) throws IOException;
  }

  private static final byte[] EMPTY = new byte[0];

  private final int partitions;
  private final int blockRows;
  private final int[] columnMap;
  private final ColumnVector[][] buffers;
  private final int[] fill;
  private final BlockSink sink;

  // per batch
  private int[] slot = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private final int[] count;

  /**
   * @param batch a batch of the source layout, for the column types
   * @param columnMap the source columns that travel, in their order in a block
   */
  public ColumnarPartitionBuffers(VectorizedRowBatch batch, int[] columnMap, int partitions, int blockRows,
      BlockSink sink) {
    this.partitions = partitions;
    this.blockRows = blockRows;
    this.columnMap = columnMap;
    this.buffers = new ColumnVector[partitions][];
    this.fill = new int[partitions];
    this.count = new int[partitions];
    this.sink = sink;
    for (int p = 0; p < partitions; p++) {
      ColumnVector[] columns = new ColumnVector[columnMap.length];
      for (int c = 0; c < columnMap.length; c++) {
        columns[c] = ColumnarShuffleBlock.newLike(batch.cols[columnMap[c]], this.blockRows);
      }
      buffers[p] = columns;
    }
  }

  /** Appends the rows of the batch (honoring its selection) to the partitions given per batch row index. */
  public void append(VectorizedRowBatch batch, int[] partition) throws IOException {
    int[] selected = batch.selectedInUse ? batch.selected : null;
    if (slot.length < batch.cols[columnMap[0]].isNull.length) {
      slot = new int[batch.cols[columnMap[0]].isNull.length];
    }
    for (int from = 0; from < batch.size; from += blockRows) {
      appendSlice(batch, selected, from, Math.min(batch.size, from + blockRows), partition);
    }
  }

  /** Appends the logical rows [from, to), no more than a block's rows, so that a partition's rows fit once emitted. */
  private void appendSlice(VectorizedRowBatch batch, int[] selected, int from, int to, int[] partition)
      throws IOException {
    Arrays.fill(count, 0);
    for (int k = from; k < to; k++) {
      count[partition[selected == null ? k : selected[k]]]++;
    }
    for (int p = 0; p < partitions; p++) {
      if (count[p] > 0 && fill[p] + count[p] > blockRows) {
        flush(p);
      }
    }
    for (int k = from; k < to; k++) {
      int i = selected == null ? k : selected[k];
      slot[i] = fill[partition[i]]++;
    }
    for (int c = 0; c < columnMap.length; c++) {
      scatter(batch.cols[columnMap[c]], c, from, to, selected, partition);
    }
  }

  /** Emits every partition that holds rows. */
  public void flushAll() throws IOException {
    for (int p = 0; p < partitions; p++) {
      if (fill[p] > 0) {
        flush(p);
      }
    }
  }

  private void flush(int p) throws IOException {
    sink.emit(p, buffers[p], fill[p]);
    for (ColumnVector column : buffers[p]) {
      column.reset();
    }
    fill[p] = 0;
  }

  private void scatter(ColumnVector source, int c, int from, int to, int[] selected, int[] partition) {
    boolean repeating = source.isRepeating;
    // nulls first: a destination with no nulls so far keeps noNulls until one arrives
    if (!source.noNulls) {
      for (int k = from; k < to; k++) {
        int i = selected == null ? k : selected[k];
        if (source.isNull[repeating ? 0 : i]) {
          ColumnVector dst = buffers[partition[i]][c];
          dst.noNulls = false;
          dst.isNull[slot[i]] = true;
        }
      }
    }
    switch (source.type) {
    case LONG: {
      long[] values = ((LongColumnVector) source).vector;
      for (int k = from; k < to; k++) {
        int i = selected == null ? k : selected[k];
        ((LongColumnVector) buffers[partition[i]][c]).vector[slot[i]] = values[repeating ? 0 : i];
      }
      break;
    }
    case DOUBLE: {
      double[] values = ((DoubleColumnVector) source).vector;
      for (int k = from; k < to; k++) {
        int i = selected == null ? k : selected[k];
        ((DoubleColumnVector) buffers[partition[i]][c]).vector[slot[i]] = values[repeating ? 0 : i];
      }
      break;
    }
    case TIMESTAMP: {
      TimestampColumnVector values = (TimestampColumnVector) source;
      for (int k = from; k < to; k++) {
        int i = selected == null ? k : selected[k];
        int src = repeating ? 0 : i;
        TimestampColumnVector dst = (TimestampColumnVector) buffers[partition[i]][c];
        dst.time[slot[i]] = values.time[src];
        dst.nanos[slot[i]] = values.nanos[src];
      }
      break;
    }
    case BYTES: {
      BytesColumnVector values = (BytesColumnVector) source;
      for (int k = from; k < to; k++) {
        int i = selected == null ? k : selected[k];
        int src = repeating ? 0 : i;
        BytesColumnVector dst = (BytesColumnVector) buffers[partition[i]][c];
        if (values.noNulls || !values.isNull[src]) {
          dst.setVal(slot[i], values.vector[src], values.start[src], values.length[src]);
        } else {
          dst.setVal(slot[i], EMPTY, 0, 0);
        }
      }
      break;
    }
    case DECIMAL: {
      DecimalColumnVector values = (DecimalColumnVector) source;
      for (int k = from; k < to; k++) {
        int i = selected == null ? k : selected[k];
        int src = repeating ? 0 : i;
        if (values.noNulls || !values.isNull[src]) {
          ((DecimalColumnVector) buffers[partition[i]][c]).vector[slot[i]].set(values.vector[src]);
        }
      }
      break;
    }
    default:
      throw new IllegalArgumentException("Unsupported column type " + source.type);
    }
  }
}
