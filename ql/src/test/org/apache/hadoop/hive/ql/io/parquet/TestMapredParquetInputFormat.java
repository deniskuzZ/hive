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
package org.apache.hadoop.hive.ql.io.parquet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import org.apache.parquet.hadoop.ParquetInputFormat;

public class TestMapredParquetInputFormat {
  @Test
  public void testDefaultConstructor() {
    new MapredParquetInputFormat();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConstructorWithParquetInputFormat() {
    new MapredParquetInputFormat(
        (ParquetInputFormat<ArrayWritable>) mock(ParquetInputFormat.class)
        );
  }


  /** The splits are FileInputFormat's, each carrying the length and modification time of its file from the listing. */
  @Test
  public void testSplitsCarryTheListedFileStatus() throws Exception {
    java.nio.file.Path dir = Files.createTempDirectory("parquet-splits");
    try {
      Files.write(dir.resolve("a.parquet"), new byte[3000]);
      Files.write(dir.resolve("b.parquet"), new byte[1000]);
      Files.write(dir.resolve("empty.parquet"), new byte[0]);
      JobConf job = new JobConf();
      FileInputFormat.setInputPaths(job, new Path(dir.toUri()));
      FileSystem fs = FileSystem.getLocal(job);
      // 10 splits over 4000 bytes: several per file
      InputSplit[] splits = new MapredParquetInputFormat().getSplits(job, 10);
      InputSplit[] expected = new FileInputFormat<NullWritable, ArrayWritable>() {
        @Override
        public RecordReader<NullWritable, ArrayWritable> getRecordReader(InputSplit split, JobConf conf,
            Reporter reporter) {
          throw new UnsupportedOperationException();
        }
      }.getSplits(job, 10);

      assertEquals(expected.length, splits.length);
      for (int i = 0; i < splits.length; i++) {
        FileSplit want = (FileSplit) expected[i];
        ParquetFileSplit split = (ParquetFileSplit) splits[i];
        assertEquals(want.getPath(), split.getPath());
        assertEquals(want.getStart(), split.getStart());
        assertEquals(want.getLength(), split.getLength());
        assertArrayEquals(want.getLocations(), split.getLocations());
        FileStatus status = fs.getFileStatus(split.getPath());
        assertEquals(status.getLen(), split.getFileLength());
        assertEquals(status.getModificationTime(), split.getModificationTime());

        ParquetFileSplit copy = roundTrip(split);
        assertEquals(split.getPath(), copy.getPath());
        assertEquals(split.getStart(), copy.getStart());
        assertEquals(split.getLength(), copy.getLength());
        assertEquals(split.getFileLength(), copy.getFileLength());
        assertEquals(split.getModificationTime(), copy.getModificationTime());
      }
    } finally {
      FileSystem.getLocal(new JobConf()).delete(new Path(dir.toUri()), true);
    }
  }

  private static ParquetFileSplit roundTrip(ParquetFileSplit split) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    split.write(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    ParquetFileSplit copy = new ParquetFileSplit();
    copy.readFields(in);
    return copy;
  }
}
