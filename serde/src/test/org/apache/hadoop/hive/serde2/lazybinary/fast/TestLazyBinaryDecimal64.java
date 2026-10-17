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
package org.apache.hadoop.hive.serde2.lazybinary.fast;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

public class TestLazyBinaryDecimal64 {
  private static final int MAX_SCALE = 20;

  /** A decimal64 is written as the HiveDecimal path writes it: same scale, same two's-complement bytes. */
  @Test
  public void testWritesTheBytesOfTheHiveDecimalPath() throws IOException {
    long absMax = HiveDecimalWritable.getDecimal64AbsMax(HiveDecimalWritable.DECIMAL64_DECIMAL_DIGITS);
    for (int scale = 0; scale <= HiveDecimalWritable.DECIMAL64_DECIMAL_DIGITS; scale++) {
      for (long value : values()) {
        if (value < -absMax || value > absMax) {
          continue;
        }
        int s = scale;
        assertArrayEquals(value + " scale " + scale,
            row(writer -> writer.writeHiveDecimal(HiveDecimal.create(BigInteger.valueOf(value), s), s)),
            row(writer -> writer.writeDecimal64(value, s)));
      }
    }
  }

  /** A decimal reads as the general path reads it, within the decimal64 range and beyond it. */
  @Test
  public void testReadsTheDecimalOfTheGeneralPath() throws IOException {
    for (int scale = 0; scale <= MAX_SCALE; scale++) {
      int s = scale;
      TypeInfo[] types = { TypeInfoFactory.getDecimalTypeInfo(HiveDecimal.MAX_PRECISION, scale) };
      for (long value : values()) {
        HiveDecimal expected = HiveDecimal.create(BigInteger.valueOf(value), scale);
        byte[] row = row(writer -> writer.writeHiveDecimal(expected, s));
        LazyBinaryDeserializeRead reader = new LazyBinaryDeserializeRead(types, false);
        reader.set(row, 0, row.length);
        assertTrue(value + " scale " + scale, reader.readNextField());
        assertEquals(value + " scale " + scale, new HiveDecimalWritable(expected), reader.currentHiveDecimalWritable);
        assertEquals(value + " scale " + scale, expected.scale(), reader.currentHiveDecimalWritable.scale());
      }
    }
  }

  /** The sign and length edges of the two's-complement bytes, every power of ten and its predecessor, and more. */
  private static List<Long> values() {
    List<Long> values = new ArrayList<>(Arrays.asList(0L, 127L, 128L, 129L, 255L, 256L, 32767L, 32768L,
        Long.MAX_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE));
    long power = 1;
    for (int k = 0; k <= 18; k++, power *= 10) {
      values.addAll(Arrays.asList(power, power - 1, power + 1, power * 7));
    }
    Random random = new Random(42);
    for (int bits = 1; bits < Long.SIZE; bits++) {
      for (int i = 0; i < 20; i++) {
        long value = random.nextLong() >>> (Long.SIZE - bits);
        values.add(value);
        values.add(value * 100);
      }
    }
    List<Long> signed = new ArrayList<>(values);
    values.forEach(value -> signed.add(-value));
    return signed;
  }

  @FunctionalInterface
  private interface Write {
    void to(LazyBinarySerializeWrite writer) throws IOException;
  }

  private static byte[] row(Write write) throws IOException {
    Output output = new Output();
    LazyBinarySerializeWrite writer = new LazyBinarySerializeWrite(1);
    writer.set(output);
    write.to(writer);
    return Arrays.copyOf(output.getData(), output.getLength());
  }
}
