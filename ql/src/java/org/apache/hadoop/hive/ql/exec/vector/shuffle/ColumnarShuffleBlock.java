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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * A block of rows of a columnar shuffle: the first rows of a set of column vectors, encoded column by column.
 * <p>
 * Layout (little endian): int rows, int columns, then per column: byte kind, byte hasNulls, the null flags (one byte per
 * row) when hasNulls, and the values of all rows (a null row's value is not meaningful): LONG and DOUBLE as 8 bytes,
 * TIMESTAMP as 8 + 4 bytes, BYTES as an int length per row then the bytes. A DECIMAL, of either vector (a
 * Decimal64ColumnVector is a LONG vector), is a byte scale and a byte wide: up to 18 digits (not wide) the unscaled
 * longs at that scale, else per row a byte scale, a byte length and the two's complement bytes of the unscaled value
 * (BigInteger's). A decimal is read into either vector, as the reducer reads a decimal of up to 18 digits as decimal64.
 */
public final class ColumnarShuffleBlock {

  /** The value TableDesc property that marks a reducer input whose records are columnar blocks. */
  public static final String INPUT_PROPERTY = "hive.vectorized.columnar.shuffle";

  static final byte LONG = 1;
  static final byte DOUBLE = 2;
  static final byte BYTES = 3;
  static final byte DECIMAL = 4;
  static final byte TIMESTAMP = 5;

  private ColumnarShuffleBlock() {
  }

  /** Whether a column of this type can travel in a block. */
  public static boolean supports(ColumnVector.Type type) {
    switch (type) {
    case LONG:
    case DOUBLE:
    case BYTES:
    case DECIMAL:
    case DECIMAL_64:
    case TIMESTAMP:
      return true;
    default:
      return false;
    }
  }

  /** The encoded size of the first rows of the columns, at most: for sizing the output. */
  public static int encodedSize(ColumnVector[] columns, int rows) {
    int size = 2 * Integer.BYTES;
    for (ColumnVector column : columns) {
      size += 2 + (column.noNulls ? 0 : rows);
      size += switch (kind(column)) {
        case LONG, DOUBLE -> rows * Long.BYTES;
        case TIMESTAMP -> rows * (Long.BYTES + Integer.BYTES);
        case BYTES -> {
          BytesColumnVector bytes = (BytesColumnVector) column;
          int n = rows * Integer.BYTES;
          for (int i = 0; i < rows; i++) {
            n += bytes.length[i];
          }
          yield n;
        }
        default -> 2 + (isWide(column) ? rows * (2 + HiveDecimal.MAX_PRECISION) : rows * Long.BYTES);
      };
    }
    return size;
  }

  /** Encodes the first rows of the columns (not repeating, no selection) into the buffer. */
  public static void encode(ColumnVector[] columns, int rows, ByteBuffer out) {
    out.order(ByteOrder.LITTLE_ENDIAN);
    out.putInt(rows);
    out.putInt(columns.length);
    for (ColumnVector column : columns) {
      byte kind = kind(column);
      out.put(kind);
      out.put((byte) (column.noNulls ? 0 : 1));
      if (!column.noNulls) {
        for (int i = 0; i < rows; i++) {
          out.put((byte) (column.isNull[i] ? 1 : 0));
        }
      }
      switch (kind) {
      case LONG:
        putLongs(out, ((LongColumnVector) column).vector, rows);
        break;
      case DOUBLE:
        out.asDoubleBuffer().put(((DoubleColumnVector) column).vector, 0, rows);
        out.position(out.position() + rows * Double.BYTES);
        break;
      case TIMESTAMP:
        TimestampColumnVector timestamps = (TimestampColumnVector) column;
        putLongs(out, timestamps.time, rows);
        out.asIntBuffer().put(timestamps.nanos, 0, rows);
        out.position(out.position() + rows * Integer.BYTES);
        break;
      case BYTES:
        BytesColumnVector bytes = (BytesColumnVector) column;
        for (int i = 0; i < rows; i++) {
          out.putInt(bytes.length[i]);
        }
        for (int i = 0; i < rows; i++) {
          out.put(bytes.vector[i], bytes.start[i], bytes.length[i]);
        }
        break;
      default:
        if (column instanceof Decimal64ColumnVector decimal64) {
          out.put((byte) decimal64.scale);
          out.put((byte) 0);
          putLongs(out, decimal64.vector, rows);
        } else {
          encodeDecimal((DecimalColumnVector) column, rows, out);
        }
        break;
      }
    }
  }

  /** Decodes a block into the first rows of the columns, which must have the block's kinds; returns the rows. */
  public static int decode(ByteBuffer in, ColumnVector[] columns) {
    in.order(ByteOrder.LITTLE_ENDIAN);
    int rows = in.getInt();
    int count = in.getInt();
    if (count != columns.length) {
      throw new IllegalStateException("Block of " + count + " columns read into " + columns.length);
    }
    for (ColumnVector column : columns) {
      byte kind = in.get();
      if (kind != kind(column)) {
        throw new IllegalStateException("Block column of kind " + kind + " read into " + column.getClass());
      }
      column.reset();
      column.ensureSize(rows, false);
      boolean hasNulls = in.get() != 0;
      if (hasNulls) {
        column.noNulls = false;
        for (int i = 0; i < rows; i++) {
          column.isNull[i] = in.get() != 0;
        }
      }
      switch (kind) {
      case LONG:
        getLongs(in, ((LongColumnVector) column).vector, rows);
        break;
      case DOUBLE:
        in.asDoubleBuffer().get(((DoubleColumnVector) column).vector, 0, rows);
        in.position(in.position() + rows * Double.BYTES);
        break;
      case TIMESTAMP:
        TimestampColumnVector timestamps = (TimestampColumnVector) column;
        getLongs(in, timestamps.time, rows);
        in.asIntBuffer().get(timestamps.nanos, 0, rows);
        in.position(in.position() + rows * Integer.BYTES);
        break;
      case BYTES:
        BytesColumnVector bytes = (BytesColumnVector) column;
        int[] lengths = new int[rows];
        for (int i = 0; i < rows; i++) {
          lengths[i] = in.getInt();
        }
        // values reference the block: no copy
        byte[] block = in.array();
        int offset = in.arrayOffset() + in.position();
        for (int i = 0; i < rows; i++) {
          bytes.setRef(i, block, offset, lengths[i]);
          offset += lengths[i];
        }
        in.position(offset - in.arrayOffset());
        break;
      default:
        decodeDecimal(in, column, rows);
        break;
      }
    }
    return rows;
  }

  private static void putLongs(ByteBuffer out, long[] values, int rows) {
    out.asLongBuffer().put(values, 0, rows);
    out.position(out.position() + rows * Long.BYTES);
  }

  private static void getLongs(ByteBuffer in, long[] values, int rows) {
    in.asLongBuffer().get(values, 0, rows);
    in.position(in.position() + rows * Long.BYTES);
  }

  /** A decimal column too wide for a long: its values travel as BigInteger bytes. */
  private static boolean isWide(ColumnVector column) {
    return column instanceof DecimalColumnVector decimals
        && decimals.precision > HiveDecimalWritable.DECIMAL64_DECIMAL_DIGITS;
  }

  private static void encodeDecimal(DecimalColumnVector decimals, int rows, ByteBuffer out) {
    out.put((byte) decimals.scale);
    boolean wide = isWide(decimals);
    out.put((byte) (wide ? 1 : 0));
    for (int i = 0; i < rows; i++) {
      boolean isNull = !decimals.noNulls && decimals.isNull[i];
      HiveDecimalWritable value = decimals.vector[i];
      if (!wide) {
        // the column's values have at most its precision and scale: exact as a long at its scale
        out.putLong(isNull ? 0 : value.serialize64(decimals.scale));
      } else if (isNull) {
        out.put((byte) 0);
        out.put((byte) 0);
      } else {
        int length = value.bigIntegerBytesInternalScratch();
        out.put((byte) value.scale());
        out.put((byte) length);
        out.put(value.bigIntegerBytesInternalScratchBuffer(), 0, length);
      }
    }
  }

  private static void decodeDecimal(ByteBuffer in, ColumnVector column, int rows) {
    int scale = in.get();
    boolean wide = in.get() != 0;
    if (column instanceof Decimal64ColumnVector decimal64) {
      if (wide || scale != decimal64.scale) {
        throw new IllegalStateException("Block decimal of scale " + scale + (wide ? ", wide," : "")
            + " read into decimal64 of scale " + decimal64.scale);
      }
      getLongs(in, decimal64.vector, rows);
      return;
    }
    DecimalColumnVector decimals = (DecimalColumnVector) column;
    if (!wide) {
      for (int i = 0; i < rows; i++) {
        long unscaled = in.getLong();
        if (decimals.noNulls || !decimals.isNull[i]) {
          decimals.vector[i].deserialize64(unscaled, scale);
        }
      }
      return;
    }
    byte[] block = in.array();
    for (int i = 0; i < rows; i++) {
      int valueScale = in.get();
      int length = in.get();
      if (decimals.noNulls || !decimals.isNull[i]) {
        decimals.vector[i].setFromBigIntegerBytesAndScale(block, in.arrayOffset() + in.position(), length,
            valueScale);
      }
      in.position(in.position() + length);
    }
  }

  /** The block kind of a column: a Decimal64ColumnVector, a LONG vector, is a DECIMAL. */
  private static byte kind(ColumnVector column) {
    if (column instanceof Decimal64ColumnVector) {
      return DECIMAL;
    }
    return switch (column.type) {
      case LONG -> LONG;
      case DOUBLE -> DOUBLE;
      case BYTES -> BYTES;
      case DECIMAL -> DECIMAL;
      case TIMESTAMP -> TIMESTAMP;
      default -> throw new IllegalArgumentException("Unsupported column type " + column.type);
    };
  }

  /** A column vector like the source, for a block of rows. */
  static ColumnVector newLike(ColumnVector source, int size) {
    if (source instanceof Decimal64ColumnVector d) {
      return new Decimal64ColumnVector(size, d.precision, d.scale);
    }
    return switch (source.type) {
      case LONG -> new LongColumnVector(size);
      case DOUBLE -> new DoubleColumnVector(size);
      case BYTES -> {
        BytesColumnVector b = new BytesColumnVector(size);
        b.initBuffer();
        yield b;
      }
      case DECIMAL -> {
        DecimalColumnVector d = (DecimalColumnVector) source;
        yield new DecimalColumnVector(size, d.precision, d.scale);
      }
      case TIMESTAMP -> new TimestampColumnVector(size);
      default -> throw new IllegalArgumentException("Unsupported column type " + source.type);
    };
  }
}
