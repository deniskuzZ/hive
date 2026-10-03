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

package org.apache.iceberg.deletes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.LongStream;
import java.util.zip.CRC32;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class TestDeletionVectors {
  private static final DeleteFile DELETE_FILE = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
      .ofPositionDeletes()
      .withPath("file:/tmp/data/deletes.parquet")
      .withFileSizeInBytes(100)
      .withRecordCount(10)
      .build();
  private static final long KEY = 1L << 32;
  private static final int CONTAINER = 1 << 16;

  private final Random random = new Random(42);

  @Test
  public void testRoundTrip() {
    PositionDeleteIndex index = DeletionVectors.deserialize(serialize(1, 5, 70_000, 5_000_000_000L), DELETE_FILE);

    Assert.assertEquals(4, index.cardinality());
    for (long position : new long[] { 1, 5, 70_000, 5_000_000_000L }) {
      Assert.assertTrue(index.isDeleted(position));
    }
    Assert.assertEquals(ImmutableList.of(DELETE_FILE), ImmutableList.copyOf(index.deleteFiles()));
  }

  @Test
  public void testCorruptedBitmapFailsTheChecksum() {
    byte[] bytes = serialize(1, 5);
    bytes[bytes.length - 5]++;

    assertInvalid(bytes, "Invalid CRC");
  }

  @Test
  public void testWrongMagicNumber() {
    byte[] bytes = serialize(1, 5);
    bytes[Integer.BYTES]++;
    CRC32 crc = new CRC32();
    crc.update(bytes, Integer.BYTES, bytes.length - 2 * Integer.BYTES);
    ByteBuffer.wrap(bytes).putInt(bytes.length - Integer.BYTES, (int) crc.getValue());

    assertInvalid(bytes, "Invalid magic number");
  }

  @Test
  public void testWrongLength() {
    byte[] bytes = serialize(1, 5);
    ByteBuffer.wrap(bytes).putInt(0, bytes.length);

    assertInvalid(bytes, "Invalid bitmap data length");
  }

  @Test
  public void testEmpty() {
    assertCursor(new TreeSet<>(), false, window(0, 10 * CONTAINER));
  }

  @Test
  public void testArrayContainers() {
    assertCursor(randomPositions(0, 4 * CONTAINER, 0.001), false, window(0, 4 * CONTAINER));
  }

  @Test
  public void testBitmapContainers() {
    assertCursor(randomPositions(0, 4 * CONTAINER, 0.07), false, window(0, 4 * CONTAINER));
    assertCursor(randomPositions(0, 4 * CONTAINER, 0.9), false, window(0, 4 * CONTAINER));
  }

  @Test
  public void testRunContainers() {
    NavigableSet<Long> positions = new TreeSet<>();
    long start = 0;
    while (start < 4 * CONTAINER) {
      long end = start + 1 + random.nextInt(5000);
      if (random.nextBoolean()) {
        LongStream.range(start, Math.min(end, 4 * CONTAINER)).forEach(positions::add);
      }
      start = end;
    }
    assertCursor(positions, true, window(0, 4 * CONTAINER));
  }

  @Test
  public void testAllDeleted() {
    NavigableSet<Long> positions = new TreeSet<>();
    LongStream.range(0, 3 * CONTAINER + 17).forEach(positions::add);
    assertCursor(positions, true, window(0, 3 * CONTAINER + 17));
  }

  @Test
  public void testContainerBoundaries() {
    NavigableSet<Long> positions = new TreeSet<>();
    for (long boundary : new long[] { CONTAINER, 2L * CONTAINER, 7L * CONTAINER }) {
      LongStream.rangeClosed(boundary - 2, boundary + 1).forEach(positions::add);
    }
    assertCursor(positions, false, window(0, 8 * CONTAINER));
  }

  @Test
  public void testPositionsBeyondIntRange() {
    // 2^31 flips the sign of the low 32 bits; 2^32 and above move to later bitmaps, leaving empty ones in between
    long[] clusters = { 0, Integer.MAX_VALUE - CONTAINER, KEY - CONTAINER, 3 * KEY + CONTAINER, 5 * KEY - CONTAINER };
    NavigableSet<Long> positions = new TreeSet<>();
    List<long[]> windows = Lists.newArrayList();
    for (long cluster : clusters) {
      positions.addAll(randomPositions(cluster, cluster + 3 * CONTAINER, 0.05));
      windows.add(new long[] { Math.max(0, cluster - 1000), cluster + 3 * CONTAINER + 1000 });
    }
    assertCursor(positions, false, windows);
    assertCursor(positions, true, windows);
  }

  @Test
  public void testSkippedRanges() {
    NavigableSet<Long> positions = randomPositions(0, 10 * CONTAINER, 0.07);
    List<long[]> windows = Lists.newArrayList();
    long start = random.nextInt(CONTAINER);
    while (start < 10 * CONTAINER) {
      long end = start + 1 + random.nextInt(3 * CONTAINER);
      windows.add(new long[] { start, end });
      start = end + random.nextInt(2 * CONTAINER);
    }
    assertCursor(positions, false, windows);
  }

  @Test
  public void testMergedIndex() {
    NavigableSet<Long> first = randomPositions(0, 3 * CONTAINER, 0.02);
    NavigableSet<Long> second = randomPositions(CONTAINER, 5 * CONTAINER, 0.3);
    PositionDeleteIndex merged = PositionDeleteIndexUtil.merge(ImmutableList.of(index(first, false),
        index(second, false), PositionDeleteIndex.empty()));

    NavigableSet<Long> expected = new TreeSet<>(first);
    expected.addAll(second);
    assertCursor(merged, expected, window(0, 5 * CONTAINER));
  }

  @Test
  public void testSparseKeys() {
    RoaringBitmap first = RoaringBitmap.bitmapOf(1);
    RoaringBitmap last = RoaringBitmap.bitmapOf(5);
    ByteBuffer blob = ByteBuffer.allocate(64 + first.serializedSizeInBytes() + last.serializedSizeInBytes());
    blob.putInt(0).putInt(0xD1D33964);
    blob.order(ByteOrder.LITTLE_ENDIAN).putLong(2).putInt(0);
    first.serialize(blob);
    blob.putInt(3);
    last.serialize(blob);
    blob.flip();

    RoaringBitmap[] bitmaps = DeletionVectors.bitmaps(blob.order(ByteOrder.BIG_ENDIAN));

    Assert.assertArrayEquals(new RoaringBitmap[] { first, new RoaringBitmap(), new RoaringBitmap(), last }, bitmaps);
  }

  private static byte[] serialize(long... positions) {
    return Deletes.toPositionIndex(CloseableIterable.withNoopClose(Arrays.stream(positions).boxed().toList()))
        .serialize().array();
  }

  private static void assertInvalid(byte[] bytes, String message) {
    IllegalArgumentException error = Assert.assertThrows(IllegalArgumentException.class,
        () -> DeletionVectors.deserialize(bytes, DELETE_FILE));
    Assert.assertTrue(error.getMessage(), error.getMessage().startsWith(message));
  }

  private static List<long[]> window(long from, long to) {
    return ImmutableList.of(new long[] { from, to });
  }

  private NavigableSet<Long> randomPositions(long from, long to, double density) {
    NavigableSet<Long> positions = new TreeSet<>();
    for (long pos = from; pos < to; pos++) {
      if (random.nextDouble() < density) {
        positions.add(pos);
      }
    }
    return positions;
  }

  private static PositionDeleteIndex index(NavigableSet<Long> positions, boolean runLengthEncode) {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    positions.forEach(bitmap::set);
    if (runLengthEncode) {
      bitmap.runLengthEncode();
    }
    return new BitmapPositionDeleteIndex(bitmap, null);
  }

  private void assertCursor(NavigableSet<Long> positions, boolean runLengthEncode, List<long[]> windows) {
    assertCursor(index(positions, runLengthEncode), positions, windows);
  }

  /**
   * Walks the ascending windows in batches of random length and checks that exactly the deleted positions inside
   * the windows are found.
   */
  private void assertCursor(PositionDeleteIndex index, NavigableSet<Long> positions, List<long[]> windows) {
    DeletionVectors.Cursor cursor = DeletionVectors.cursor(index);
    List<Long> found = Lists.newArrayList();
    List<Long> expected = Lists.newArrayList();
    for (long[] window : windows) {
      expected.addAll(positions.subSet(window[0], window[1]));
      long from = window[0];
      while (from < window[1]) {
        long to = Math.min(window[1], from + 1 + random.nextInt(2048));
        for (long pos = cursor.nextDeleted(from, to); pos < to; pos = cursor.nextDeleted(pos + 1, to)) {
          Assert.assertTrue(index.isDeleted(pos));
          found.add(pos);
        }
        from = to;
      }
    }
    Assert.assertEquals(expected, found);
  }
}
