/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.vector;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.iceberg.deletes.DeletionVectors;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Removes the rows of a data file's position deletes (delete files or a deletion vector) from its row batches.
 */
class HiveDeleteFilter {

  private final DeletionVectors.Cursor deletes;

  HiveDeleteFilter(PositionDeleteIndex deletes) {
    this.deletes = DeletionVectors.cursor(deletes);
  }

  /**
   * Adjusts the pipeline of incoming VRBs so that the deleted rows of each batch are filtered out.
   * @param batches iterable of HiveBatchContexts i.e. VRBs and their meta information, in file order
   * @return the adjusted iterable of HiveBatchContexts
   */
  CloseableIterable<HiveBatchContext> filterBatch(CloseableIterable<HiveBatchContext> batches) {
    return CloseableIterable.transform(batches, context -> {
      filter(context.getBatch(), context.getFileRowOffset());
      return context;
    });
  }

  /**
   * Drops the deleted rows from the batch's selection, where row {@code i} of the batch is at file position
   * {@code fileRowOffset + i}. Batches must arrive in ascending file position order.
   */
  void filter(VectorizedRowBatch batch, long fileRowOffset) {
    int size = batch.size;
    if (size == 0) {
      return;
    }
    if (fileRowOffset == Long.MIN_VALUE) {
      throw new UnsupportedOperationException("Can't provide row position for batch.");
    }
    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;
    long end = fileRowOffset + (selectedInUse ? selected[size - 1] + 1 : size);
    long deleted = deletes.nextDeleted(fileRowOffset + (selectedInUse ? selected[0] : 0), end);
    if (deleted == end) {
      return;
    }
    int newSize = 0;
    for (int i = 0; i < size; i++) {
      int row = selectedInUse ? selected[i] : i;
      long pos = fileRowOffset + row;
      if (deleted < pos) {
        deleted = deletes.nextDeleted(pos, end);
      }
      if (deleted != pos) {
        selected[newSize++] = row;
      }
    }
    batch.size = newSize;
    batch.selectedInUse = true;
  }
}
