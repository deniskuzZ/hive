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

import org.apache.hadoop.hive.ql.util.JavaDataModel;

import com.google.common.annotations.VisibleForTesting;

/**
 * Hash table from a single bytes key (STRING, CHAR, VARCHAR, BINARY) to the aggregation buffers of
 * its group, used by the vectorized hash GROUP BY.
 * <p>
 * Open addressing with linear probing over a power-of-two slot array that is at most half full.
 * Entries are numbered in insertion order. Each entry keeps the first 16 key bytes as two
 * zero-padded little-endian words next to the key hash and length, so a probe decides equality of
 * keys up to 16 bytes long from one record; longer keys also compare the remaining bytes. That
 * measured 1.5-1.7x faster than comparing the key bytes on 412 short keys. The key bytes are
 * copied once, when the entry is added. The NULL key has an entry outside the slot array.
 * <p>
 * A removed entry keeps its number until {@link #compactIfSparse} drops it, and its slot, as a
 * tombstone that matches no key, until the table grows or compacts.
 */
final class VectorGroupByBytesKeyTable {

  /**
   * Follows the renumbering of the remaining entries by a compaction.
   */
  interface EntryMover {
    /**
     * Moves entry entries[i] to entry i for each i below count, the entries being increasing.
     */
    void moveEntries(int[] entries, int count);
  }

  private static final VarHandle LONG_LE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private static final int INITIAL_CAPACITY = 16;

  // Record of an entry in entryWords: low word, high word, hash << 32 | length.
  private static final int ENTRY_WORDS = 3;

  private static final int WORDS_LENGTH = 2 * Long.BYTES;

  // Entry + 1, 0 when empty.
  private int[] slots = new int[2 * INITIAL_CAPACITY];
  private long[] entryWords = new long[ENTRY_WORDS * INITIAL_CAPACITY];
  private byte[][] keys = new byte[INITIAL_CAPACITY][];
  private VectorAggregationBufferRow[] rows = new VectorAggregationBufferRow[INITIAL_CAPACITY];
  // Number of entries, including the removed ones.
  private int end;
  private int size;
  private int nullEntry = -1;
  private long keysMemorySize;

  /**
   * Returns the number of entries, excluding the removed ones.
   */
  @VisibleForTesting
  int size() {
    return size;
  }

  /**
   * Returns the number of entries, including the removed ones.
   */
  int end() {
    return end;
  }

  /**
   * Returns the entry of the key, adding one when absent. A new entry has no aggregation buffers
   * until {@link #setRow}.
   */
  int findOrAdd(byte[] bytes, int start, int length) {
    final long low;
    final long high;
    if (start + WORDS_LENGTH <= bytes.length) {
      low = (long) LONG_LE.get(bytes, start) & lowBytesMask(length);
      high = (long) LONG_LE.get(bytes, start + Long.BYTES) & lowBytesMask(length - Long.BYTES);
    } else {
      low = word(bytes, start, length);
      high = word(bytes, start + Long.BYTES, length - Long.BYTES);
    }
    final int hash = hash(low, high, bytes, start, length);
    final long hashAndLength = ((long) hash << 32) | length;
    final int mask = slots.length - 1;
    int slot = hash & mask;
    for (int entry; (entry = slots[slot] - 1) >= 0; slot = (slot + 1) & mask) {
      final int record = ENTRY_WORDS * entry;
      if (entryWords[record + 2] == hashAndLength
          && entryWords[record] == low
          && entryWords[record + 1] == high
          && (length <= WORDS_LENGTH || Arrays.equals(keys[entry], WORDS_LENGTH, length,
              bytes, start + WORDS_LENGTH, start + length))) {
        return entry;
      }
    }
    final int entry = end++;
    final int record = ENTRY_WORDS * entry;
    entryWords[record] = low;
    entryWords[record + 1] = high;
    entryWords[record + 2] = hashAndLength;
    keys[entry] = Arrays.copyOfRange(bytes, start, start + length);
    keysMemorySize += JavaDataModel.get().lengthForByteArrayOfSize(length);
    slots[slot] = entry + 1;
    size++;
    if (end == rows.length) {
      resize(2 * rows.length);
    }
    return entry;
  }

  /**
   * Returns the entry of the NULL key, adding one when absent.
   */
  int findOrAddNull() {
    if (nullEntry < 0) {
      nullEntry = end++;
      size++;
      if (end == rows.length) {
        resize(2 * rows.length);
      }
    }
    return nullEntry;
  }

  VectorAggregationBufferRow getRow(int entry) {
    return rows[entry];
  }

  void setRow(int entry, VectorAggregationBufferRow row) {
    rows[entry] = row;
  }

  /**
   * Returns whether the entry was removed.
   */
  boolean isRemoved(int entry) {
    return keys[entry] == null && entry != nullEntry;
  }

  /**
   * Sets the key of the entry at the given row of the column.
   */
  void writeKey(int entry, BytesColumnVector column, int row) {
    if (entry == nullEntry) {
      column.noNulls = false;
      column.isNull[row] = true;
    } else {
      // The key bytes are never modified, so the column can reference them.
      column.isNull[row] = false;
      column.setRef(row, keys[entry], 0, keys[entry].length);
    }
  }

  /**
   * Removes the entry. Its key record gets a length no key has, so its slot is a tombstone that
   * lookups probe past.
   */
  void remove(int entry) {
    if (entry == nullEntry) {
      nullEntry = -1;
    } else {
      keysMemorySize -= JavaDataModel.get().lengthForByteArrayOfSize(keys[entry].length);
      keys[entry] = null;
      entryWords[ENTRY_WORDS * entry + 2] = -1;
    }
    rows[entry] = null;
    size--;
  }

  /**
   * Drops the removed entries once they outnumber the others or fill the free room, so the table
   * grows only for the remaining entries. Renumbers the remaining entries in order and rebuilds the
   * table from them, keeping the capacity, as the table refills after a partial flush, unless they
   * fill more than 3/4 of it: then the capacity doubles, as the next compaction would otherwise
   * follow after a few removals and rebuild the whole table for them. A sparse table compacts after
   * at least as many removals as there are remaining entries. The mover, if any, follows the
   * renumbering.
   */
  void compactIfSparse(EntryMover mover) {
    if (2 * size >= end && end - size < rows.length - end) {
      return;
    }
    int kept = 0;
    // A local, not the field: storing the field in this loop is ~2.7x slower under G1.
    int keptNullEntry = -1;
    // Recorded even without a mover: a conditional store here measured 3.5x slower under C2.
    final int[] keptEntries = new int[size];
    for (int entry = 0; entry < end; entry++) {
      if (isRemoved(entry)) {
        continue;
      }
      if (entry == nullEntry) {
        keptNullEntry = kept;
      }
      keptEntries[kept] = entry;
      System.arraycopy(entryWords, ENTRY_WORDS * entry, entryWords, ENTRY_WORDS * kept,
          ENTRY_WORDS);
      keys[kept] = keys[entry];
      rows[kept] = rows[entry];
      kept++;
    }
    Arrays.fill(keys, kept, end, null);
    Arrays.fill(rows, kept, end, null);
    end = kept;
    nullEntry = keptNullEntry;
    if (mover != null) {
      // One pass per array of the mover: moving all of them per entry measured 10x slower.
      mover.moveEntries(keptEntries, kept);
    }
    resize(4 * kept > 3 * rows.length ? 2 * rows.length : rows.length);
  }

  /**
   * Removes all entries.
   */
  void clear() {
    Arrays.fill(slots, 0);
    Arrays.fill(keys, 0, end, null);
    Arrays.fill(rows, 0, end, null);
    end = 0;
    size = 0;
    nullEntry = -1;
    keysMemorySize = 0;
  }

  /**
   * Returns the estimated memory of the table arrays and the key bytes.
   */
  long getMemorySize() {
    JavaDataModel model = JavaDataModel.get();
    return model.lengthForIntArrayOfSize(slots.length)
        + model.lengthForLongArrayOfSize(entryWords.length)
        + 2 * model.lengthForObjectArrayOfSize(rows.length)
        + keysMemorySize;
  }

  private void resize(int newCapacity) {
    if (newCapacity == rows.length) {
      Arrays.fill(slots, 0);
    } else {
      entryWords = Arrays.copyOf(entryWords, ENTRY_WORDS * newCapacity);
      keys = Arrays.copyOf(keys, newCapacity);
      rows = Arrays.copyOf(rows, newCapacity);
      slots = new int[2 * newCapacity];
    }
    final int mask = slots.length - 1;
    for (int entry = 0; entry < end; entry++) {
      // Skips the NULL entry and the removed ones.
      if (keys[entry] == null) {
        continue;
      }
      int slot = (int) (entryWords[ENTRY_WORDS * entry + 2] >>> 32) & mask;
      while (slots[slot] != 0) {
        slot = (slot + 1) & mask;
      }
      slots[slot] = entry + 1;
    }
  }

  /**
   * Returns the mask of the low {@code length} bytes of a word, all of them for a length above 8
   * and none for a negative one. Branch free, as key lengths vary from row to row.
   */
  private static long lowBytesMask(int length) {
    final int bytes = Math.max(0, Math.min(length, Long.BYTES));
    return ~(-1L << (bytes << 3)) | -(bytes >> 3);
  }

  /**
   * Returns the {@code length} bytes at {@code offset} as a zero-padded little-endian word, reading
   * at most 8 bytes and none when {@code length} is not positive.
   */
  private static long word(byte[] bytes, int offset, int length) {
    if (length <= 0) {
      return 0;
    }
    if (offset + Long.BYTES <= bytes.length) {
      final long word = (long) LONG_LE.get(bytes, offset);
      return length >= Long.BYTES ? word : word & ((1L << (length << 3)) - 1);
    }
    long word = 0;
    for (int i = Math.min(length, Long.BYTES) - 1; i >= 0; i--) {
      word = (word << 8) | (bytes[offset + i] & 0xFFL);
    }
    return word;
  }

  @VisibleForTesting
  static int hash(byte[] bytes, int start, int length) {
    return hash(word(bytes, start, length), word(bytes, start + Long.BYTES, length - Long.BYTES),
        bytes, start, length);
  }

  private static int hash(long low, long high, byte[] bytes, int start, int length) {
    long hash =
        low * 0x9E3779B97F4A7C15L ^ Long.rotateLeft(high * 0xC2B2AE3D27D4EB4FL, 29) ^ length;
    for (int i = WORDS_LENGTH; i < length; i += Long.BYTES) {
      hash = Long.rotateLeft(hash ^ word(bytes, start + i, length - i) * 0xC2B2AE3D27D4EB4FL, 31)
          * 0x9E3779B97F4A7C15L;
    }
    hash ^= hash >>> 29;
    hash *= 0xBF58476D1CE4E5B9L;
    return (int) (hash ^ (hash >>> 32));
  }
}
