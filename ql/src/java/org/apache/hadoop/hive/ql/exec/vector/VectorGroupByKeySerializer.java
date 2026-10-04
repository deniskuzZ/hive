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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Serializes the GROUP BY keys of the rows of a batch, made of two or more LONG, BYTES and DECIMAL
 * columns, into one byte array for a {@link VectorGroupByBytesKeyTable}, and writes a serialized key
 * back into the key columns of an output batch. A batch is serialized a column at a time. A DECIMAL
 * column of at most {@link HiveDecimalWritable#DECIMAL64_DECIMAL_DIGITS} digits is a LONG column of
 * its decimal64 values at the column's scale, so equal decimals have equal values.
 * <p>
 * A key is the little-endian value of each LONG column, in 4 bytes when it fits in an int and in
 * 8 otherwise, then the bytes of each BYTES column, then the 4-byte little-endian length of each
 * BYTES column but the last, then one bit per column that is set when the column is NULL, then one
 * bit per LONG column that is set when its value takes 8 bytes. A NULL column has no bytes. Every
 * value has a single serialization, so two rows have the same key exactly when their key columns
 * are equal, a NULL being equal to a NULL.
 * <p>
 * A key that fits in 16 bytes is compared from its table record alone, so 4-byte values measured
 * 1.1-2.0x faster than 8-byte ones on two and three LONG columns. The serialization measured
 * 1.4-2.4x faster than {@link VectorSerializeRow} with BinarySortableSerializeWrite or
 * LazyBinarySerializeWrite, which serialize a row at a time.
 */
final class VectorGroupByKeySerializer {

  private static final VarHandle LONG_LE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_LE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

  private final int[] columnNums;
  private final int columnCount;
  // Positions in the key of the LONG (DECIMAL included) and of the BYTES columns.
  private final int[] longKeys;
  private final int[] bytesKeys;
  // Per key, the scale of a DECIMAL column and its decimal64 values in the batch; -1 and null otherwise.
  private final int[] decimalScales;
  private final LongColumnVector[] decimalValues;
  private final int bitsLength;
  // The lengths of the BYTES columns and the bits.
  private final int trailerLength;

  private byte[] bytes = new byte[VectorizedRowBatch.DEFAULT_SIZE * 32];
  // Per row of the batch, the offset and length of its key.
  private final int[] starts = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private final int[] lengths = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private final int[] ends = new int[VectorizedRowBatch.DEFAULT_SIZE];
  // Per row of the batch, where its next column goes.
  private final int[] offsets = new int[VectorizedRowBatch.DEFAULT_SIZE];

  /**
   * Returns whether keys of columns of these types can be serialized.
   */
  static boolean covers(ColumnVector.Type[] types, TypeInfo[] typeInfos) {
    for (int key = 0; key < types.length; key++) {
      if (types[key] != ColumnVector.Type.BYTES && !isLong(types[key], typeInfos[key])) {
        return false;
      }
    }
    return types.length > 1;
  }

  private static boolean isLong(ColumnVector.Type type, TypeInfo typeInfo) {
    return type == ColumnVector.Type.LONG || type == ColumnVector.Type.DECIMAL
        && HiveDecimalWritable.isPrecisionDecimal64(((DecimalTypeInfo) typeInfo).getPrecision());
  }

  VectorGroupByKeySerializer(ColumnVector.Type[] types, TypeInfo[] typeInfos, int[] columnNums) {
    this.columnNums = columnNums;
    columnCount = types.length;
    int longCount = 0;
    for (int key = 0; key < types.length; key++) {
      if (isLong(types[key], typeInfos[key])) {
        longCount++;
      }
    }
    longKeys = new int[longCount];
    bytesKeys = new int[types.length - longCount];
    decimalScales = new int[types.length];
    decimalValues = new LongColumnVector[types.length];
    for (int key = 0, l = 0, b = 0; key < types.length; key++) {
      decimalScales[key] = -1;
      if (isLong(types[key], typeInfos[key])) {
        longKeys[l++] = key;
        if (types[key] == ColumnVector.Type.DECIMAL) {
          decimalScales[key] = ((DecimalTypeInfo) typeInfos[key]).getScale();
          decimalValues[key] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
        }
      } else {
        bytesKeys[b++] = key;
      }
    }
    bitsLength = (types.length + longCount + 7) / Byte.SIZE;
    trailerLength = Integer.BYTES * Math.max(0, bytesKeys.length - 1) + bitsLength;
  }

  byte[] getBytes() {
    return bytes;
  }

  int[] getStarts() {
    return starts;
  }

  int[] getLengths() {
    return lengths;
  }

  /**
   * Serializes the keys of the rows of the batch.
   */
  void serialize(VectorizedRowBatch batch) {
    final int size = batch.size;
    final boolean selectedInUse = batch.selectedInUse;
    final int[] selected = batch.selected;
    final int[] lengths = this.lengths;
    Arrays.fill(lengths, 0, size, trailerLength);
    for (int key : longKeys) {
      if (decimalValues[key] != null) {
        toDecimal64((DecimalColumnVector) batch.cols[columnNums[key]], decimalScales[key], decimalValues[key], size,
            selectedInUse, selected);
      }
    }
    for (int key : longKeys) {
      final LongColumnVector column = longColumn(batch, key);
      final long[] vector = column.vector;
      if (column.isRepeating) {
        final int width = !column.noNulls && column.isNull[0] ? 0 : width(vector[0]);
        for (int i = 0; i < size; i++) {
          lengths[i] += width;
        }
      } else {
        final boolean noNulls = column.noNulls;
        final boolean[] isNull = column.isNull;
        for (int i = 0; i < size; i++) {
          final int row = selectedInUse ? selected[i] : i;
          if (noNulls || !isNull[row]) {
            lengths[i] += width(vector[row]);
          }
        }
      }
    }
    for (int key : bytesKeys) {
      final BytesColumnVector column = (BytesColumnVector) batch.cols[columnNums[key]];
      if (column.isRepeating) {
        final int length = column.noNulls || !column.isNull[0] ? column.length[0] : 0;
        for (int i = 0; i < size; i++) {
          lengths[i] += length;
        }
      } else {
        final int[] length = column.length;
        final boolean noNulls = column.noNulls;
        final boolean[] isNull = column.isNull;
        for (int i = 0; i < size; i++) {
          final int row = selectedInUse ? selected[i] : i;
          if (noNulls || !isNull[row]) {
            lengths[i] += length[row];
          }
        }
      }
    }
    final int[] starts = this.starts;
    final int[] ends = this.ends;
    int total = 0;
    for (int i = 0; i < size; i++) {
      starts[i] = total;
      total += lengths[i];
      ends[i] = total;
    }
    if (bytes.length < total) {
      bytes = new byte[Math.max(total, 2 * bytes.length)];
    }
    final byte[] bytes = this.bytes;
    for (int i = 0; i < size; i++) {
      Arrays.fill(bytes, ends[i] - bitsLength, ends[i], (byte) 0);
    }
    final int[] offsets = this.offsets;
    System.arraycopy(starts, 0, offsets, 0, size);
    for (int l = 0; l < longKeys.length; l++) {
      serializeLongs(longColumn(batch, longKeys[l]), longKeys[l],
          columnCount + l, size, selectedInUse, selected, offsets);
    }
    for (int b = 0; b < bytesKeys.length; b++) {
      // Where the length of the column is, from the end of the key; 0 for the last column.
      final int lengthFromEnd = b < bytesKeys.length - 1 ? trailerLength - Integer.BYTES * b : 0;
      serializeBytes((BytesColumnVector) batch.cols[columnNums[bytesKeys[b]]], bytesKeys[b],
          lengthFromEnd, size, selectedInUse, selected, offsets);
    }
  }

  private LongColumnVector longColumn(VectorizedRowBatch batch, int key) {
    return decimalValues[key] != null ? decimalValues[key] : (LongColumnVector) batch.cols[columnNums[key]];
  }

  private static void toDecimal64(DecimalColumnVector column, int scale, LongColumnVector values, int size,
      boolean selectedInUse, int[] selected) {
    values.noNulls = column.noNulls;
    values.isRepeating = column.isRepeating;
    final int count = column.isRepeating ? 1 : size;
    for (int i = 0; i < count; i++) {
      final int row = column.isRepeating ? 0 : selectedInUse ? selected[i] : i;
      values.isNull[row] = !column.noNulls && column.isNull[row];
      if (!values.isNull[row]) {
        values.vector[row] = column.vector[row].serialize64(scale);
      }
    }
  }

  private static int width(long value) {
    return (int) value == value ? Integer.BYTES : Long.BYTES;
  }

  private void serializeLongs(LongColumnVector column, int key, int wideBit, int size,
      boolean selectedInUse, int[] selected, int[] offsets) {
    final byte[] bytes = this.bytes;
    final long[] vector = column.vector;
    final boolean noNulls = column.noNulls;
    final boolean[] isNull = column.isNull;
    final boolean isRepeating = column.isRepeating;
    for (int i = 0; i < size; i++) {
      final int row = isRepeating ? 0 : selectedInUse ? selected[i] : i;
      if (noNulls || !isNull[row]) {
        final long value = vector[row];
        if ((int) value == value) {
          INT_LE.set(bytes, offsets[i], (int) value);
          offsets[i] += Integer.BYTES;
        } else {
          LONG_LE.set(bytes, offsets[i], value);
          offsets[i] += Long.BYTES;
          setBit(i, wideBit);
        }
      } else {
        setBit(i, key);
      }
    }
  }

  private void serializeBytes(BytesColumnVector column, int key, int lengthFromEnd, int size,
      boolean selectedInUse, int[] selected, int[] offsets) {
    final byte[] bytes = this.bytes;
    final byte[][] vector = column.vector;
    final int[] start = column.start;
    final int[] length = column.length;
    final boolean noNulls = column.noNulls;
    final boolean[] isNull = column.isNull;
    final boolean isRepeating = column.isRepeating;
    for (int i = 0; i < size; i++) {
      final int row = isRepeating ? 0 : selectedInUse ? selected[i] : i;
      int valueLength = 0;
      if (noNulls || !isNull[row]) {
        valueLength = length[row];
        System.arraycopy(vector[row], start[row], bytes, offsets[i], valueLength);
        offsets[i] += valueLength;
      } else {
        setBit(i, key);
      }
      if (lengthFromEnd > 0) {
        INT_LE.set(bytes, ends[i] - lengthFromEnd, valueLength);
      }
    }
  }

  private void setBit(int i, int bit) {
    bytes[ends[i] - bitsLength + bit / Byte.SIZE] |= (byte) (1 << (bit % Byte.SIZE));
  }

  /**
   * Sets the key columns, the first columns of the batch, at the given row from a key. BYTES
   * columns reference the key, so its bytes must not be modified.
   */
  void deserialize(byte[] key, VectorizedRowBatch batch, int row) {
    final int bitsStart = key.length - bitsLength;
    int offset = 0;
    for (int l = 0; l < longKeys.length; l++) {
      final int k = longKeys[l];
      final ColumnVector column = batch.cols[k];
      if (isSet(key, bitsStart, k)) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        column.isNull[row] = false;
        final long value;
        if (isSet(key, bitsStart, columnCount + l)) {
          value = (long) LONG_LE.get(key, offset);
          offset += Long.BYTES;
        } else {
          value = (int) INT_LE.get(key, offset);
          offset += Integer.BYTES;
        }
        if (decimalScales[k] >= 0) {
          ((DecimalColumnVector) column).vector[row].deserialize64(value, decimalScales[k]);
        } else {
          ((LongColumnVector) column).vector[row] = value;
        }
      }
    }
    final int lengthsStart = key.length - trailerLength;
    for (int b = 0; b < bytesKeys.length; b++) {
      final int length = b < bytesKeys.length - 1
          ? (int) INT_LE.get(key, lengthsStart + Integer.BYTES * b) : lengthsStart - offset;
      final BytesColumnVector column = (BytesColumnVector) batch.cols[bytesKeys[b]];
      if (isSet(key, bitsStart, bytesKeys[b])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        column.isNull[row] = false;
        column.setRef(row, key, offset, length);
      }
      offset += length;
    }
  }

  private boolean isSet(byte[] key, int bitsStart, int bit) {
    return (key[bitsStart + bit / Byte.SIZE] & (1 << (bit % Byte.SIZE))) != 0;
  }
}
