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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableDeserializeRead;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sorted runs of a hash GROUP BY over an unsorted input, which holds every group of its partition: when memory runs
 * short, its groups are written in partial form to a local file in the order of the hash of their key, straight from
 * the hash table; at close the runs are merged by hash into batches, the keys of one hash in key order, so equal keys
 * are adjacent for a streaming merge of the partial results. A key is serialized BinarySortable, so byte order is key
 * order, its partial results LazyBinary.
 */
public final class VectorGroupBySpill {
  private static final Logger LOG = LoggerFactory.getLogger(VectorGroupBySpill.class);

  /** Receives the merged batches. */
  public interface BatchConsumer {
    void accept(VectorizedRowBatch batch) throws HiveException;
  }

  private final String localDirs;
  private final int keyCount;
  private final TypeInfo[] keyTypes;
  private final DataTypePhysicalVariation[] keyVariations;
  private final TypeInfo[] valueTypes;
  private final DataTypePhysicalVariation[] valueVariations;
  private final VectorSerializeRow<BinarySortableSerializeWrite> keySerializer;
  private final VectorSerializeRow<LazyBinarySerializeWrite> valueSerializer;
  private final Output keyOutput = new Output();
  private final Output valueOutput = new Output();
  private File dir;
  private final List<File> runs = new ArrayList<>();
  private DataOutputStream run;

  /**
   * The runs take their rows from batches whose first columns are the keys and the next ones the partial results.
   */
  public VectorGroupBySpill(Configuration conf, TypeInfo[] keyTypes, DataTypePhysicalVariation[] keyVariations,
      TypeInfo[] valueTypes, DataTypePhysicalVariation[] valueVariations) throws HiveException {
    this.localDirs = HiveUtils.getLocalDirList(conf);
    this.keyCount = keyTypes.length;
    this.keyTypes = keyTypes;
    this.keyVariations = keyVariations;
    this.valueTypes = valueTypes;
    this.valueVariations = valueVariations;
    keySerializer = new VectorSerializeRow<>(new BinarySortableSerializeWrite(keyCount));
    keySerializer.init(keyTypes, range(0, keyCount));
    valueSerializer = new VectorSerializeRow<>(new LazyBinarySerializeWrite(valueTypes.length));
    valueSerializer.init(valueTypes, range(keyCount, keyCount + valueTypes.length));
  }

  public boolean hasRuns() {
    return !runs.isEmpty();
  }

  /**
   * Starts a run.
   */
  public void startRun() throws HiveException {
    try {
      if (dir == null) {
        dir = FileUtils.createLocalDirsTempFile(localDirs, "vector-groupby-spill", "", true);
      }
      File file = File.createTempFile("run", ".run", dir);
      runs.add(file);
      run = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file), 1 << 16));
    } catch (IOException e) {
      throw new HiveException("Cannot spill GROUP BY run", e);
    }
  }

  /**
   * Writes the rows of a batch to the run, given the hashes of their keys; a run takes its rows in hash order.
   */
  public void add(VectorizedRowBatch batch, int[] hashes) throws HiveException {
    try {
      for (int i = 0; i < batch.size; i++) {
        int row = batch.selectedInUse ? batch.selected[i] : i;
        keyOutput.reset();
        keySerializer.setOutput(keyOutput);
        keySerializer.serializeWrite(batch, row);
        valueOutput.reset();
        valueSerializer.setOutput(valueOutput);
        valueSerializer.serializeWrite(batch, row);
        run.writeInt(keyOutput.getLength());
        run.writeInt(hashes[row]);
        run.write(keyOutput.getData(), 0, keyOutput.getLength());
        run.writeInt(valueOutput.getLength());
        run.write(valueOutput.getData(), 0, valueOutput.getLength());
      }
    } catch (IOException e) {
      throw new HiveException("Cannot spill GROUP BY row", e);
    }
  }

  /**
   * Ends the run.
   */
  public void endRun() throws HiveException {
    try {
      run.writeInt(-1);
      run.close();
      run = null;
      LOG.info("GROUP BY spilled run {} to {}", runs.size(), runs.get(runs.size() - 1));
    } catch (IOException e) {
      throw new HiveException("Cannot spill GROUP BY run", e);
    }
  }

  /**
   * Merges the runs by key into batches handed to the consumer, with the keys and the partial results in the given
   * columns of the batch, then deletes them.
   */
  public void merge(VectorizedRowBatch batch, int[] keyColumns, int[] valueColumns, BatchConsumer consumer)
      throws HiveException {
    boolean[] ascending = new boolean[keyCount];
    byte[] nullMarkers = new byte[keyCount];
    Arrays.fill(nullMarkers, BinarySortableSerDe.ZERO);
    byte[] notNullMarkers = new byte[keyCount];
    Arrays.fill(notNullMarkers, BinarySortableSerDe.ONE);
    VectorDeserializeRow<BinarySortableDeserializeRead> keyDeserializer = new VectorDeserializeRow<>(
        new BinarySortableDeserializeRead(keyTypes, keyVariations, false, ascending, nullMarkers, notNullMarkers));
    keyDeserializer.init(keyColumns);
    VectorDeserializeRow<LazyBinaryDeserializeRead> valueDeserializer = new VectorDeserializeRow<>(
        new LazyBinaryDeserializeRead(valueTypes, valueVariations, false));
    valueDeserializer.init(valueColumns);

    PriorityQueue<RunReader> queue = new PriorityQueue<>(Comparator.comparingInt(r -> r.hash));
    List<byte[][]> group = new ArrayList<>();
    try {
      for (File run : runs) {
        RunReader reader = new RunReader(run);
        if (reader.next()) {
          queue.add(reader);
        } else {
          reader.close();
        }
      }
      batch.reset();
      while (!queue.isEmpty()) {
        // the rows of the next hash in every run; distinct keys of a hash are put in key order
        int hash = queue.peek().hash;
        while (!queue.isEmpty() && queue.peek().hash == hash) {
          RunReader reader = queue.poll();
          boolean more;
          do {
            group.add(new byte[][] {reader.key, reader.value});
            more = reader.next();
          } while (more && reader.hash == hash);
          if (more) {
            queue.add(reader);
          } else {
            reader.close();
          }
        }
        if (group.size() > 1) {
          group.sort((a, b) -> compare(a[0], b[0]));
        }
        for (byte[][] row : group) {
          keyDeserializer.setBytes(row[0], 0, row[0].length);
          keyDeserializer.deserialize(batch, batch.size);
          valueDeserializer.setBytes(row[1], 0, row[1].length);
          valueDeserializer.deserialize(batch, batch.size);
          if (++batch.size == batch.getMaxSize()) {
            consumer.accept(batch);
            batch.reset();
          }
        }
        group.clear();
      }
      if (batch.size > 0) {
        consumer.accept(batch);
        batch.reset();
      }
    } catch (IOException e) {
      throw new HiveException("Cannot merge GROUP BY runs", e);
    } finally {
      for (RunReader reader : queue) {
        reader.closeQuietly();
      }
      deleteRuns();
    }
  }

  /**
   * Deletes the runs of an aborted aggregation.
   */
  public void discard() {
    if (run != null) {
      try {
        run.close();
      } catch (IOException ignored) {
        // the run is deleted next
      }
      run = null;
    }
    deleteRuns();
  }

  private void deleteRuns() {
    if (dir != null) {
      FileUtil.fullyDelete(dir);
      dir = null;
    }
    runs.clear();
  }

  private static int compare(byte[] a, byte[] b) {
    return WritableComparator.compareBytes(a, 0, a.length, b, 0, b.length);
  }

  private static int[] range(int from, int to) {
    int[] columns = new int[to - from];
    Arrays.setAll(columns, i -> from + i);
    return columns;
  }

  private static final class RunReader {
    private final DataInputStream in;
    private int hash;
    private byte[] key;
    private byte[] value;

    RunReader(File file) throws IOException {
      in = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 1 << 16));
    }

    boolean next() throws IOException {
      int keyLength = in.readInt();
      if (keyLength < 0) {
        return false;
      }
      hash = in.readInt();
      key = new byte[keyLength];
      in.readFully(key);
      value = new byte[in.readInt()];
      in.readFully(value);
      return true;
    }

    void close() throws IOException {
      in.close();
    }

    void closeQuietly() {
      try {
        in.close();
      } catch (IOException ignored) {
        // the run is deleted next
      }
    }
  }
}
