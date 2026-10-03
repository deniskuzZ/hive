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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.zip.CRC32;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * Hive-side helpers for Iceberg position delete indexes.
 */
public final class DeletionVectors {
  private static final int MAGIC_NUMBER = 0x6439D3D1;

  private DeletionVectors() {
  }

  /**
   * Iceberg has no public deletion vector deserializer that does not validate against a DV DeleteFile; this can go
   * once it offers one.
   * <p>
   * Reads the {@link PositionDeleteIndex#serialize()} format into an index attributed to the given delete file.
   */
  public static PositionDeleteIndex deserialize(byte[] bytes, DeleteFile deleteFile) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int bitmapDataLength = buffer.getInt();
    Preconditions.checkArgument(bitmapDataLength == bytes.length - 2 * Integer.BYTES,
        "Invalid bitmap data length: %s", bitmapDataLength);
    CRC32 crc = new CRC32();
    crc.update(bytes, Integer.BYTES, bitmapDataLength);
    Preconditions.checkArgument((int) crc.getValue() == buffer.getInt(Integer.BYTES + bitmapDataLength),
        "Invalid CRC");
    ByteBuffer bitmapData = ByteBuffer.wrap(bytes, Integer.BYTES, bitmapDataLength).order(ByteOrder.LITTLE_ENDIAN);
    int magicNumber = bitmapData.getInt();
    Preconditions.checkArgument(magicNumber == MAGIC_NUMBER, "Invalid magic number: %s", magicNumber);
    return new BitmapPositionDeleteIndex(RoaringPositionBitmap.deserialize(bitmapData), deleteFile);
  }

  /**
   * Iceberg has no public ranged iterator over a {@link PositionDeleteIndex} before
   * {@code PositionDeleteIndex#forEachInRange} (1.12, apache/iceberg#18027); this can go once Hive is on it.
   * <p>
   * Reads the index through its deletion vector serialization, which run-length encodes the index in place: the
   * index must be exclusively owned by the caller, such as a fresh one from {@code DeleteLoader#loadPositionDeletes}.
   */
  public static Cursor cursor(PositionDeleteIndex index) {
    return new Cursor(bitmaps(index.serialize()));
  }

  /**
   * Reads the 32-bit bitmaps of a serialized deletion vector, indexed by their keys.
   */
  static RoaringBitmap[] bitmaps(ByteBuffer serialized) {
    ByteBuffer buffer = serialized.order(ByteOrder.LITTLE_ENDIAN);
    // skip the length and the magic number
    buffer.position(buffer.position() + 2 * Integer.BYTES);
    long bitmapCount = buffer.getLong();
    List<RoaringBitmap> bitmaps = Lists.newArrayList();
    try {
      for (long i = 0; i < bitmapCount; i++) {
        int key = buffer.getInt();
        // keys may be sparse
        while (bitmaps.size() < key) {
          bitmaps.add(new RoaringBitmap());
        }
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.deserialize(buffer);
        buffer.position(buffer.position() + bitmap.serializedSizeInBytes());
        bitmaps.add(bitmap);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bitmaps.toArray(new RoaringBitmap[0]);
  }

  /**
   * Walks the deleted positions of an index in ascending order. The positions passed to successive
   * {@link #nextDeleted} calls must not decrease.
   */
  public static final class Cursor {
    // Bitmap of the low 32 bits of the positions, indexed by their high 32 bits.
    private final RoaringBitmap[] bitmaps;
    private int key = -1;
    private PeekableIntIterator iterator;

    private Cursor(RoaringBitmap[] bitmaps) {
      this.bitmaps = bitmaps;
    }

    /**
     * Returns the smallest deleted position in [from, to), or {@code to} if there is none.
     */
    public long nextDeleted(long from, long to) {
      long pos = from;
      while (pos < to) {
        int high = (int) (pos >>> 32);
        if (high >= bitmaps.length) {
          return to;
        }
        if (high != key) {
          key = high;
          iterator = bitmaps[high].getIntIterator();
        }
        iterator.advanceIfNeeded((int) pos);
        if (iterator.hasNext()) {
          return Math.min(((long) high << 32) | Integer.toUnsignedLong(iterator.peekNext()), to);
        }
        pos = (long) (high + 1) << 32;
      }
      return to;
    }
  }
}
