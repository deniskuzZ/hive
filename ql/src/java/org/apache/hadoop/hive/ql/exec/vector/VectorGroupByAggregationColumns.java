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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCount;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCountStar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFSumDecimal64;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxDecimal64;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinDecimal64;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFSumLong;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

/**
 * The aggregation state of the entries of a {@link VectorGroupByBytesKeyTable}, kept in one
 * primitive array per aggregate indexed by entry, and the access count of each entry. A batch is
 * aggregated with one loop per aggregate over the entries of its rows, instead of through an
 * aggregation buffer object per entry and aggregate.
 * <p>
 * Covers min and max of LONG and DECIMAL_64, sum of LONG and DECIMAL_64, count and count(*), with
 * the results of {@link VectorUDAFMinLong}, {@link VectorUDAFMaxLong},
 * {@link VectorUDAFMinDecimal64}, {@link VectorUDAFMaxDecimal64}, {@link VectorUDAFSumLong},
 * {@link VectorUDAFSumDecimal64}, {@link VectorUDAFCount} and {@link VectorUDAFCountStar}:
 * <ul>
 *   <li>min, max and sum of a group without a non-NULL value are NULL, count is 0;</li>
 *   <li>a DECIMAL_64 sum that exceeds its output precision at any row is NULL;</li>
 *   <li>the DECIMAL_64 min and max start from Long.MAX_VALUE and Long.MIN_VALUE, which no
 *   DECIMAL_64 value reaches, so a group keeps one of them exactly when it has no value.</li>
 * </ul>
 */
final class VectorGroupByAggregationColumns implements VectorGroupByBytesKeyTable.EntryMover {

  private enum Kind {
    MIN, MAX, MIN_DECIMAL64, MAX_DECIMAL64, SUM, SUM_DECIMAL64, COUNT, COUNT_STAR
  }

  private static final int INITIAL_CAPACITY = 16;

  private final Kind[] kinds;
  private final VectorExpression[] inputs;
  // Per aggregate, the largest absolute DECIMAL_64 sum of its output precision.
  private final long[] maxAbsSums;

  private int capacity;
  // Per aggregate, the value of each entry.
  private final long[][] values;
  // Per aggregate, whether the entry has a value; null for aggregates that need no flag.
  private final boolean[][] hasValues;
  // Per aggregate, whether the DECIMAL_64 sum of the entry overflowed; null for the others.
  private final boolean[][] overflows;
  private int[] accessCounts;

  private VectorGroupByAggregationColumns(Kind[] kinds, VectorExpression[] inputs,
      long[] maxAbsSums) {
    this.kinds = kinds;
    this.inputs = inputs;
    this.maxAbsSums = maxAbsSums;
    values = new long[kinds.length][];
    hasValues = new boolean[kinds.length][];
    overflows = new boolean[kinds.length][];
    for (int i = 0; i < kinds.length; i++) {
      values[i] = new long[0];
      if (kinds[i] == Kind.MIN || kinds[i] == Kind.MAX || kinds[i] == Kind.SUM
          || kinds[i] == Kind.SUM_DECIMAL64) {
        hasValues[i] = new boolean[0];
      }
      if (kinds[i] == Kind.SUM_DECIMAL64) {
        overflows[i] = new boolean[0];
      }
    }
    accessCounts = new int[0];
    grow(INITIAL_CAPACITY);
  }

  /**
   * Returns the columns for the aggregators, or null when one of them is not covered.
   */
  static VectorGroupByAggregationColumns create(VectorAggregateExpression[] aggregators) {
    Kind[] kinds = new Kind[aggregators.length];
    VectorExpression[] inputs = new VectorExpression[aggregators.length];
    long[] maxAbsSums = new long[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      VectorAggregateExpression aggregator = aggregators[i];
      kinds[i] = kind(aggregator.getClass());
      if (kinds[i] == null) {
        return null;
      }
      inputs[i] = aggregator.getInputExpression();
      if (kinds[i] == Kind.SUM_DECIMAL64) {
        maxAbsSums[i] = HiveDecimalWritable.getDecimal64AbsMax(
            ((DecimalTypeInfo) aggregator.getOutputTypeInfo()).getPrecision());
      }
    }
    return new VectorGroupByAggregationColumns(kinds, inputs, maxAbsSums);
  }

  private static Kind kind(Class<?> aggregatorClass) {
    if (aggregatorClass == VectorUDAFMinLong.class) {
      return Kind.MIN;
    } else if (aggregatorClass == VectorUDAFMaxLong.class) {
      return Kind.MAX;
    } else if (aggregatorClass == VectorUDAFMinDecimal64.class) {
      return Kind.MIN_DECIMAL64;
    } else if (aggregatorClass == VectorUDAFMaxDecimal64.class) {
      return Kind.MAX_DECIMAL64;
    } else if (aggregatorClass == VectorUDAFSumLong.class) {
      return Kind.SUM;
    } else if (aggregatorClass == VectorUDAFSumDecimal64.class) {
      return Kind.SUM_DECIMAL64;
    } else if (aggregatorClass == VectorUDAFCount.class) {
      return Kind.COUNT;
    } else if (aggregatorClass == VectorUDAFCountStar.class) {
      return Kind.COUNT_STAR;
    }
    return null;
  }

  /**
   * Starts the entries [from, to) without values and with an access count of -1, as the row that
   * adds an entry is not an access.
   */
  void initEntries(int from, int to) {
    if (to > capacity) {
      grow(Math.max(to, 2 * capacity));
    }
    for (int i = 0; i < kinds.length; i++) {
      Arrays.fill(values[i], from, to, initialValue(kinds[i]));
      if (hasValues[i] != null) {
        Arrays.fill(hasValues[i], from, to, false);
      }
      if (overflows[i] != null) {
        Arrays.fill(overflows[i], from, to, false);
      }
    }
    Arrays.fill(accessCounts, from, to, -1);
  }

  private static long initialValue(Kind kind) {
    switch (kind) {
    case MIN:
    case MIN_DECIMAL64:
      return Long.MAX_VALUE;
    case MAX:
    case MAX_DECIMAL64:
      return Long.MIN_VALUE;
    default:
      return 0;
    }
  }

  private void grow(int newCapacity) {
    for (int i = 0; i < kinds.length; i++) {
      values[i] = Arrays.copyOf(values[i], newCapacity);
      if (hasValues[i] != null) {
        hasValues[i] = Arrays.copyOf(hasValues[i], newCapacity);
      }
      if (overflows[i] != null) {
        overflows[i] = Arrays.copyOf(overflows[i], newCapacity);
      }
    }
    accessCounts = Arrays.copyOf(accessCounts, newCapacity);
    capacity = newCapacity;
  }

  @Override
  public void moveEntries(int[] entries, int count) {
    for (int i = 0; i < kinds.length; i++) {
      final long[] v = values[i];
      for (int j = 0; j < count; j++) {
        v[j] = v[entries[j]];
      }
      if (hasValues[i] != null) {
        moveEntries(hasValues[i], entries, count);
      }
      if (overflows[i] != null) {
        moveEntries(overflows[i], entries, count);
      }
    }
    final int[] counts = accessCounts;
    for (int j = 0; j < count; j++) {
      counts[j] = counts[entries[j]];
    }
  }

  private static void moveEntries(boolean[] flags, int[] entries, int count) {
    for (int j = 0; j < count; j++) {
      flags[j] = flags[entries[j]];
    }
  }

  int getAccessCount(int entry) {
    return accessCounts[entry];
  }

  void resetAccessCount(int entry) {
    accessCounts[entry] = 0;
  }

  /**
   * Returns the estimated memory of the arrays.
   */
  long getMemorySize() {
    JavaDataModel model = JavaDataModel.get();
    long size = model.lengthForIntArrayOfSize(capacity);
    for (int i = 0; i < kinds.length; i++) {
      size += model.lengthForLongArrayOfSize(capacity);
      if (hasValues[i] != null) {
        size += model.lengthForBooleanArrayOfSize(capacity);
      }
      if (overflows[i] != null) {
        size += model.lengthForBooleanArrayOfSize(capacity);
      }
    }
    return size;
  }

  /**
   * Aggregates the rows of the batch, the i-th of them into entry entries[i], and counts an
   * access per row.
   */
  void aggregate(VectorizedRowBatch batch, int[] entries) throws HiveException {
    final int size = batch.size;
    if (size == 0) {
      return;
    }
    final int[] selected = batch.selectedInUse ? batch.selected : null;
    final int[] counts = accessCounts;
    for (int i = 0; i < size; i++) {
      counts[entries[i]]++;
    }
    for (int a = 0; a < kinds.length; a++) {
      if (kinds[a] == Kind.COUNT_STAR) {
        countRows(values[a], entries, size);
        continue;
      }
      inputs[a].evaluate(batch);
      ColumnVector input = batch.cols[inputs[a].getOutputColumnNum()];
      if (input.isRepeating) {
        if (input.noNulls || !input.isNull[0]) {
          if (kinds[a] == Kind.COUNT) {
            countRows(values[a], entries, size);
          } else {
            aggregateRepeating(a, ((LongColumnVector) input).vector[0], entries, size);
          }
        }
      } else if (kinds[a] == Kind.COUNT) {
        countNonNulls(values[a], input, selected, entries, size);
      } else if (input.noNulls) {
        aggregateNoNulls(a, ((LongColumnVector) input).vector, selected, entries, size);
      } else {
        aggregateNullable(a, (LongColumnVector) input, selected, entries, size);
      }
    }
  }

  private static void countRows(long[] counts, int[] entries, int size) {
    for (int i = 0; i < size; i++) {
      counts[entries[i]]++;
    }
  }

  private static void countNonNulls(long[] counts, ColumnVector input, int[] selected,
      int[] entries, int size) {
    if (input.noNulls) {
      countRows(counts, entries, size);
      return;
    }
    final boolean[] isNull = input.isNull;
    for (int i = 0; i < size; i++) {
      if (!isNull[selected == null ? i : selected[i]]) {
        counts[entries[i]]++;
      }
    }
  }

  private void aggregateRepeating(int a, long value, int[] entries, int size) {
    final long[] v = values[a];
    switch (kinds[a]) {
    case MIN:
    case MIN_DECIMAL64:
      for (int i = 0; i < size; i++) {
        final int e = entries[i];
        v[e] = Math.min(v[e], value);
      }
      break;
    case MAX:
    case MAX_DECIMAL64:
      for (int i = 0; i < size; i++) {
        final int e = entries[i];
        v[e] = Math.max(v[e], value);
      }
      break;
    case SUM:
      for (int i = 0; i < size; i++) {
        v[entries[i]] += value;
      }
      break;
    case SUM_DECIMAL64:
      final long maxAbsSum = maxAbsSums[a];
      final boolean[] overflow = overflows[a];
      for (int i = 0; i < size; i++) {
        final int e = entries[i];
        final long sum = v[e] + value;
        v[e] = sum;
        if (Math.abs(sum) > maxAbsSum) {
          overflow[e] = true;
        }
      }
      break;
    default:
      throw new IllegalStateException(kinds[a].toString());
    }
    markHasValue(hasValues[a], entries, size);
  }

  private void aggregateNoNulls(int a, long[] input, int[] selected, int[] entries, int size) {
    final long[] v = values[a];
    switch (kinds[a]) {
    case MIN:
    case MIN_DECIMAL64:
      if (selected == null) {
        for (int i = 0; i < size; i++) {
          final int e = entries[i];
          v[e] = Math.min(v[e], input[i]);
        }
      } else {
        for (int i = 0; i < size; i++) {
          final int e = entries[i];
          v[e] = Math.min(v[e], input[selected[i]]);
        }
      }
      break;
    case MAX:
    case MAX_DECIMAL64:
      if (selected == null) {
        for (int i = 0; i < size; i++) {
          final int e = entries[i];
          v[e] = Math.max(v[e], input[i]);
        }
      } else {
        for (int i = 0; i < size; i++) {
          final int e = entries[i];
          v[e] = Math.max(v[e], input[selected[i]]);
        }
      }
      break;
    case SUM:
      if (selected == null) {
        for (int i = 0; i < size; i++) {
          v[entries[i]] += input[i];
        }
      } else {
        for (int i = 0; i < size; i++) {
          v[entries[i]] += input[selected[i]];
        }
      }
      break;
    case SUM_DECIMAL64:
      final long maxAbsSum = maxAbsSums[a];
      final boolean[] overflow = overflows[a];
      if (selected == null) {
        for (int i = 0; i < size; i++) {
          final int e = entries[i];
          final long sum = v[e] + input[i];
          v[e] = sum;
          if (Math.abs(sum) > maxAbsSum) {
            overflow[e] = true;
          }
        }
      } else {
        for (int i = 0; i < size; i++) {
          final int e = entries[i];
          final long sum = v[e] + input[selected[i]];
          v[e] = sum;
          if (Math.abs(sum) > maxAbsSum) {
            overflow[e] = true;
          }
        }
      }
      break;
    default:
      throw new IllegalStateException(kinds[a].toString());
    }
    markHasValue(hasValues[a], entries, size);
  }

  private static void markHasValue(boolean[] hasValue, int[] entries, int size) {
    if (hasValue != null) {
      for (int i = 0; i < size; i++) {
        hasValue[entries[i]] = true;
      }
    }
  }

  private void aggregateNullable(int a, LongColumnVector input, int[] selected, int[] entries,
      int size) {
    final long[] v = values[a];
    final long[] vector = input.vector;
    final boolean[] isNull = input.isNull;
    final boolean[] hasValue = hasValues[a];
    final Kind kind = kinds[a];
    for (int i = 0; i < size; i++) {
      final int row = selected == null ? i : selected[i];
      if (isNull[row]) {
        continue;
      }
      final int e = entries[i];
      final long value = vector[row];
      switch (kind) {
      case MIN:
      case MIN_DECIMAL64:
        v[e] = Math.min(v[e], value);
        break;
      case MAX:
      case MAX_DECIMAL64:
        v[e] = Math.max(v[e], value);
        break;
      case SUM:
        v[e] += value;
        break;
      case SUM_DECIMAL64:
        v[e] += value;
        if (Math.abs(v[e]) > maxAbsSums[a]) {
          overflows[a][e] = true;
        }
        break;
      default:
        throw new IllegalStateException(kind.toString());
      }
      if (hasValue != null) {
        hasValue[e] = true;
      }
    }
  }

  /**
   * Sets the aggregates of the entry at the given row of the output columns, the first aggregate
   * in column firstColumn.
   */
  void write(int entry, VectorizedRowBatch output, int row, int firstColumn) {
    for (int a = 0; a < kinds.length; a++) {
      LongColumnVector column = (LongColumnVector) output.cols[firstColumn + a];
      if (isNull(a, entry)) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        column.isNull[row] = false;
        column.vector[row] = values[a][entry];
      }
    }
  }

  private boolean isNull(int a, int entry) {
    switch (kinds[a]) {
    case MIN_DECIMAL64:
      return values[a][entry] == Long.MAX_VALUE;
    case MAX_DECIMAL64:
      return values[a][entry] == Long.MIN_VALUE;
    case SUM_DECIMAL64:
      return !hasValues[a][entry] || overflows[a][entry];
    case COUNT:
    case COUNT_STAR:
      return false;
    default:
      return !hasValues[a][entry];
    }
  }
}
