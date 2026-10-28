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
import org.apache.hadoop.hive.ql.exec.vector.shuffle.ColumnarShuffleWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;

/**
 * The columnar shuffle (ColumnarShuffleWriter) of a ReduceSink of an unsorted edge that partitions by columns other
 * than its key: an empty key, as the DISTRIBUTE BY of an Iceberg merge-on-read DML. A row goes to the partition the row
 * path gives it; a batch whose partition columns all repeat is hashed once and leaves as one block, its repeating
 * columns (a delete's FILE__PATH, PARTITION__SPEC__ID, PARTITION__KEY) as their one value.
 */
public class VectorReduceSinkColumnarObjectHashOperator extends VectorReduceSinkObjectHashOperator {

  private static final long serialVersionUID = 1L;

  private transient ColumnarShuffleWriter writer;
  private transient int[] partitionOfRow;

  /** Kryo ctor. */
  protected VectorReduceSinkColumnarObjectHashOperator() {
    super();
  }

  public VectorReduceSinkColumnarObjectHashOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorReduceSinkColumnarObjectHashOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    super(ctx, conf, vContext, vectorDesc);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    partitionOfRow = new int[VectorizedRowBatch.DEFAULT_SIZE];
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) row;
    batchCounter++;
    if (batch.size == 0) {
      return;
    }
    if (reduceSinkValueExpressions != null) {
      for (VectorExpression ve : reduceSinkValueExpressions) {
        ve.evaluate(batch);
      }
    }
    if (reduceSinkPartitionExpressions != null) {
      for (VectorExpression ve : reduceSinkPartitionExpressions) {
        ve.evaluate(batch);
      }
    }
    try {
      if (writer == null) {
        // the column types are known with the first batch; the key is empty, so a block holds the value columns
        writer = new ColumnarShuffleWriter(batch, reduceSinkValueColumnMap, outputPartitions(), this::collectBlock);
      }
      if (isRepeating(batch, reduceSinkPartitionColumnMap)) {
        writer.write(batch, writer.partitionOf(partitionHashCode(batch, 0)));
      } else {
        int capacity = batch.cols[reduceSinkValueColumnMap[0]].isNull.length;
        if (partitionOfRow.length < capacity) {
          partitionOfRow = new int[capacity];
        }
        for (int logical = 0; logical < batch.size; logical++) {
          int batchIndex = batch.selectedInUse ? batch.selected[logical] : logical;
          partitionOfRow[batchIndex] = writer.partitionOf(partitionHashCode(batch, batchIndex));
        }
        writer.write(batch, partitionOfRow);
      }
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
