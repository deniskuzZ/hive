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

package org.apache.hadoop.hive.common.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

public class TestNonSyncByteArrayOutputStream {

  @Test
  public void testWritesGrowTheBuffer() {
    NonSyncByteArrayOutputStream out = new NonSyncByteArrayOutputStream(4);
    byte[] expected = new byte[17];
    for (int i = 0; i < 10; i++) {
      out.write(i);
      expected[i] = (byte) i;
    }
    // a bulk write that fits only after growing, then one that fills the buffer exactly
    byte[] bulk = {10, 11, 12, 13, 14, 15, 16};
    out.write(bulk, 0, bulk.length);
    System.arraycopy(bulk, 0, expected, 10, bulk.length);
    int left = out.getData().length - out.getLength();
    byte[] fill = new byte[left];
    Arrays.fill(fill, (byte) 99);
    out.write(fill, 0, left);
    // the buffer is full: a single byte grows it
    out.write(100);
    assertEquals(17 + left + 1, out.getLength());
    assertArrayEquals(expected, Arrays.copyOf(out.getData(), 17));
    assertEquals(99, out.getData()[17 + left - 1]);
    assertEquals(100, out.getData()[17 + left]);
  }

  @Test
  public void testResetKeepsTheBuffer() {
    NonSyncByteArrayOutputStream out = new NonSyncByteArrayOutputStream(2);
    out.write(1);
    out.write(2);
    out.write(3);
    out.reset();
    out.write(7);
    assertEquals(1, out.getLength());
    assertEquals(7, out.getData()[0]);
  }
}
