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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Test;

public class TestColumnarShuffleBlock {

  private static final int COLUMNS = 7;

  /**
   * Random batches (nulls, repeating columns, selections) appended to random partitions come back from the encoded
   * blocks as each partition's rows in order, for blocks of a batch and smaller.
   */
  @Test
  public void testRoundTripPerPartition() throws Exception {
    Random random = new Random(7);
    int[][] shapes = {{1, 1024}, {7, 1024}, {1009, 1024}, {3, 100}, {1009, 64}};
    for (int[] shape : shapes) {
      int partitions = shape[0];
      int blockRows = shape[1];
      VectorizedRowBatch batch = newBatch();
      List<List<String>> expected = new ArrayList<>();
      List<List<String>> actual = new ArrayList<>();
      for (int p = 0; p < partitions; p++) {
        expected.add(new ArrayList<>());
        actual.add(new ArrayList<>());
      }
      ColumnVector[] decoded = newBatch().cols;
      ColumnarPartitionBuffers buffers = new ColumnarPartitionBuffers(batch, new int[] {0, 1, 2, 3, 4, 5, 6},
          partitions, blockRows, (partition, columns, rows) -> {
            assertTrue(rows <= blockRows);
            ByteBuffer block = ByteBuffer.allocate(ColumnarShuffleBlock.encodedSize(columns, rows));
            ColumnarShuffleBlock.encode(columns, rows, block);
            block.flip();
            int n = ColumnarShuffleBlock.decode(block, decoded);
            assertEquals(rows, n);
            for (int i = 0; i < n; i++) {
              actual.get(partition).add(row(decoded, i));
            }
          });
      int[] partition = new int[VectorizedRowBatch.DEFAULT_SIZE];
      for (int b = 0; b < 40; b++) {
        fill(batch, random);
        for (int i = 0; i < batch.size; i++) {
          partition[i] = random.nextInt(partitions);
        }
        if (random.nextInt(3) == 0) {
          batch.selectedInUse = true;
          int n = 0;
          for (int i = 0; i < batch.size; i++) {
            if (random.nextBoolean()) {
              batch.selected[n++] = i;
            }
          }
          batch.size = n;
        } else {
          batch.selectedInUse = false;
        }
        for (int k = 0; k < batch.size; k++) {
          int i = batch.selectedInUse ? batch.selected[k] : k;
          expected.get(partition[i]).add(row(batch.cols, i));
        }
        buffers.append(batch, partition);
      }
      buffers.flushAll();
      assertEquals(expected, actual);
    }
  }

  /**
   * A decimal travels between its two vectors, as a reducer reads a decimal of up to 18 digits as decimal64 whichever
   * vector the ReduceSink holds it in.
   */
  @Test
  public void testDecimalAcrossVectors() {
    String[] values = {"123.45", "-1.5", null, "99999.99", "0"};
    Decimal64ColumnVector decimal64 = new Decimal64ColumnVector(values.length, 7, 2);
    DecimalColumnVector decimal = new DecimalColumnVector(values.length, 7, 2);
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        decimal64.noNulls = decimal.noNulls = false;
        decimal64.isNull[i] = decimal.isNull[i] = true;
      } else {
        decimal64.vector[i] = HiveDecimal.create(values[i]).scaleByPowerOfTen(2).longValueExact();
        decimal.set(i, HiveDecimal.create(values[i]));
      }
    }
    DecimalColumnVector toDecimal = new DecimalColumnVector(7, 2);
    roundTrip(decimal64, toDecimal, values.length);
    Decimal64ColumnVector toDecimal64 = new Decimal64ColumnVector(7, 2);
    roundTrip(decimal, toDecimal64, values.length);
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i] == null, toDecimal.isNull[i]);
      assertEquals(values[i] == null, toDecimal64.isNull[i]);
      if (values[i] != null) {
        assertEquals(HiveDecimal.create(values[i]), toDecimal.vector[i].getHiveDecimal());
        assertEquals(decimal64.vector[i], toDecimal64.vector[i]);
      }
    }
  }

  private static void roundTrip(ColumnVector source, ColumnVector target, int rows) {
    ColumnVector[] columns = {source};
    ByteBuffer block = ByteBuffer.allocate(ColumnarShuffleBlock.encodedSize(columns, rows));
    ColumnarShuffleBlock.encode(columns, rows, block);
    block.flip();
    assertEquals(rows, ColumnarShuffleBlock.decode(block, new ColumnVector[] {target}));
  }

  private static VectorizedRowBatch newBatch() {
    VectorizedRowBatch batch = new VectorizedRowBatch(COLUMNS);
    batch.cols[0] = new LongColumnVector();
    batch.cols[1] = new Decimal64ColumnVector(17, 2);
    batch.cols[2] = new DoubleColumnVector();
    batch.cols[3] = new BytesColumnVector();
    batch.cols[4] = new DecimalColumnVector(38, 10);
    batch.cols[5] = new TimestampColumnVector();
    batch.cols[6] = new DecimalColumnVector(17, 2);
    return batch;
  }

  private static void fill(VectorizedRowBatch batch, Random random) {
    int n = 1 + random.nextInt(VectorizedRowBatch.DEFAULT_SIZE);
    batch.size = n;
    for (ColumnVector column : batch.cols) {
      column.reset();
      boolean repeating = random.nextInt(5) == 0;
      boolean nulls = random.nextInt(3) == 0;
      for (int i = 0; i < (repeating ? 1 : n); i++) {
        if (nulls && random.nextInt(4) == 0) {
          column.noNulls = false;
          column.isNull[i] = true;
          continue;
        }
        long v = random.nextLong() >>> random.nextInt(64);
        switch (column.type) {
        case LONG, DECIMAL_64 -> ((LongColumnVector) column).vector[i] = v % 100000000000L;
        case DOUBLE -> ((DoubleColumnVector) column).vector[i] = v / 7.0;
        case BYTES -> ((BytesColumnVector) column).setVal(i, ("s" + v).getBytes(StandardCharsets.UTF_8));
        case DECIMAL -> {
          DecimalColumnVector decimals = (DecimalColumnVector) column;
          // signed, of every magnitude the column holds, with trailing zeroes the value drops
          BigInteger unscaled = new BigInteger(random.nextInt(decimals.precision * 3 + 1), random);
          if (unscaled.compareTo(BigInteger.TEN.pow(decimals.precision)) >= 0) {
            unscaled = unscaled.mod(BigInteger.TEN.pow(decimals.precision));
          }
          if (random.nextBoolean()) {
            unscaled = unscaled.negate();
          }
          decimals.set(i, HiveDecimal.create(new BigDecimal(unscaled, random.nextInt(decimals.scale + 1))));
        }
        case TIMESTAMP -> ((TimestampColumnVector) column).set(i, new Timestamp(v % 4000000000000L));
        default -> throw new IllegalStateException();
        }
      }
      column.isRepeating = repeating;
    }
  }

  private static String row(ColumnVector[] columns, int i) {
    StringBuilder sb = new StringBuilder();
    for (ColumnVector column : columns) {
      int r = column.isRepeating ? 0 : i;
      if (!column.noNulls && column.isNull[r]) {
        sb.append("NULL|");
        continue;
      }
      switch (column.type) {
      case LONG, DECIMAL_64 -> sb.append(((LongColumnVector) column).vector[r]);
      case DOUBLE -> sb.append(((DoubleColumnVector) column).vector[r]);
      case BYTES -> {
        BytesColumnVector b = (BytesColumnVector) column;
        sb.append(new String(b.vector[r], b.start[r], b.length[r], StandardCharsets.UTF_8));
      }
      case DECIMAL -> sb.append(((DecimalColumnVector) column).vector[r].getHiveDecimal());
      case TIMESTAMP -> {
        TimestampColumnVector t = (TimestampColumnVector) column;
        sb.append(t.time[r]).append('.').append(t.nanos[r]);
      }
      default -> throw new IllegalStateException();
      }
      sb.append('|');
    }
    return sb.toString();
  }
}
