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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * A split of a Parquet file that carries the length and modification time of the file from the listing that made it,
 * so a reader can identify the file and open it without asking the file system for its status.
 */
public class ParquetFileSplit extends FileSplit {

  private long fileLength;
  private long modificationTime;

  /** For deserialization. */
  public ParquetFileSplit() {
  }

  public ParquetFileSplit(Path file, long start, long length, String[] hosts, FileStatus status) {
    super(file, start, length, hosts);
    this.fileLength = status.getLen();
    this.modificationTime = status.getModificationTime();
  }

  public ParquetFileSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts,
      FileStatus status) {
    super(file, start, length, hosts, inMemoryHosts);
    this.fileLength = status.getLen();
    this.modificationTime = status.getModificationTime();
  }

  public long getFileLength() {
    return fileLength;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  /**
   * The status of the file as the listing gave it: its length and modification time, nothing else.
   */
  public FileStatus getFileStatus() {
    return new FileStatus(fileLength, false, 0, 0, modificationTime, getPath());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(fileLength);
    out.writeLong(modificationTime);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    fileLength = in.readLong();
    modificationTime = in.readLong();
  }
}
