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

package org.apache.iceberg.mr.hive.writer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hive.ql.Context.Operation;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;

public class WriterBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(WriterBuilder.class);
  private final Table table;
  private final Supplier<Map<String, DeleteFileSet>> rewritableDeletes;
  private final Context context;
  private final String tableName;
  private TaskAttemptID attemptID;
  private String queryId;
  private Operation operation;
  private final boolean copyOnWrite;

  // A task may write multiple output files using multiple writers. Each of them must have a unique operationId.
  private static AtomicInteger operationNum = new AtomicInteger(0);

  // To specify whether to write the actual row data while writing the delete files.
  public static final String ICEBERG_DELETE_SKIPROWDATA = "iceberg.delete.skiprowdata";
  public static final boolean ICEBERG_DELETE_SKIPROWDATA_DEFAULT = true;
  private boolean shouldAddRowLineageColumns = false;
  private Supplier<ObjectCache> objectCache = () -> null;

  private WriterBuilder(Table table, UnaryOperator<String> ops) {
    this.table = table;
    this.tableName = ops.apply(Catalogs.NAME);
    this.context = new Context(table.properties(), ops, tableName);
    this.operation = HiveCustomStorageHandlerUtils.getWriteOperation(ops, tableName);
    this.copyOnWrite = HiveCustomStorageHandlerUtils.isCopyOnWrite(ops, tableName);
    this.rewritableDeletes = () -> rewritableDeletes(ops);
  }

  public static WriterBuilder builderFor(Table table, UnaryOperator<String> ops) {
    return new WriterBuilder(table, ops);
  }

  public WriterBuilder attemptID(TaskAttemptID newAttemptID) {
    this.attemptID = newAttemptID;
    return this;
  }

  public WriterBuilder queryId(String newQueryId) {
    this.queryId = newQueryId;
    return this;
  }

  // Test-only
  public WriterBuilder operation(Operation newOperation) {
    this.operation = newOperation;
    return this;
  }

  public HiveIcebergWriter build() {
    int partitionId = attemptID.getTaskID().getId();
    int taskId = attemptID.getId();
    String operationId = queryId + "-" + attemptID.getJobID() + "-" + operationNum.incrementAndGet();

    OutputFileFactory dataFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
        .format(context.dataFileFormat())
        .operationId(operationId)
        .build();

    OutputFileFactory deleteFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
        .format(context.deleteFileFormat())
        .operationId(operationId)
        .suffix("pos-deletes")
        .build();

    HiveFileWriterFactory writerFactory = HiveFileWriterFactory.builderFor(table)
        .dataFileFormat(context.dataFileFormat())
        .dataSchema(shouldAddRowLineageColumns ? MetadataColumns.schemaWithRowLineage(table.schema()) : table.schema())
        .deleteFileFormat(context.deleteFileFormat())
        .positionDeleteRowSchema(context.skipRowData() || !context.inputOrdered() ?
            // SortingPositionOnlyDeleteWriter doesn't support rawData in delete schema
            null : table.schema())
        .build();

    HiveIcebergWriter writer;
    if (copyOnWrite) {
      writer = new HiveIcebergCopyOnWriteRecordWriter(table, writerFactory, dataFileFactory, shouldAddRowLineageColumns,
          context);
    } else {
      writer = switch (operation) {
        case DELETE ->
            new HiveIcebergDeleteWriter(table, rewritableDeletes.get(), writerFactory, deleteFileFactory, context);
        case OTHER ->
            new HiveIcebergRecordWriter(table, writerFactory, dataFileFactory, context);
        default ->
            // Update and Merge should be split to inserts and deletes
            throw new IllegalArgumentException("Unsupported operation when creating IcebergRecordWriter: " +
                operation.name());
      };
    }

    WriterRegistry.registerWriter(attemptID, tableName, writer);
    return writer;
  }

  private Map<String, DeleteFileSet> rewritableDeletes(UnaryOperator<String> ops) {
    String location = ops.apply(InputFormatConfig.REWRITABLE_DELETES_PREFIX + tableName);
    if (location != null) {
      return loadRewritableDeletes(location);
    }
    Preconditions.checkState(!context.useDVs(),
        "The rewritable deletes of the DV writers of %s were not planned for the job", tableName);
    // the target ref of the write, a reducer does not see the ref of the scan
    String ref = HiveUtils.getTableSnapshotRef(ops.apply(Catalogs.SNAPSHOT_REF));
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, ref);
    boolean caseSensitive = ObjectUtils.defaultIfNull(
        Boolean.parseBoolean(ops.apply(InputFormatConfig.CASE_SENSITIVE)),
        InputFormatConfig.CASE_SENSITIVE_DEFAULT);
    Expression filterExpression = SerializationUtil.deserializeFromBase64(
        ops.apply(InputFormatConfig.FILTER_EXPRESSION));

    BatchScan scan = table.newBatchScan().useSnapshot(snapshot.snapshotId())
        .caseSensitive(caseSensitive);
    if (filterExpression != null) {
      scan = scan.filter(filterExpression);
    }
    if (shouldRewriteDeletes()) {
      return rewritableDeletes(scan, context.useDVs());
    }
    return null;
  }

  /**
   * Plans the rewritable deletes of the DV writers of a table, and stores them at the given location.
   */
  public static void writeRewritableDeletes(Table table, String ref, String location) {
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, HiveUtils.getTableSnapshotRef(ref));
    Map<String, DeleteFileSet> deletes = snapshot == null ? Maps.newHashMap() :
        rewritableDeletes(table.newBatchScan().useSnapshot(snapshot.snapshotId()), true);
    try (ObjectOutputStream out = new ObjectOutputStream(
        new GZIPOutputStream(table.io().newOutputFile(location).create()))) {
      out.writeObject(deletes);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write rewritable deletes to %s", location), e);
    }
    LOG.info("Planned rewritable deletes of {} data files to {}", deletes.size(), location);
  }

  // the writers of a query share the rewritable deletes of the job
  private Map<String, DeleteFileSet> loadRewritableDeletes(String location) {
    ObjectCache cache = objectCache.get();
    if (cache == null) {
      return readRewritableDeletes(table.io(), location);
    }
    try {
      return cache.<RewritableDeletes>retrieve(location,
          () -> new RewritableDeletes(readRewritableDeletes(table.io(), location))).deletes();
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, DeleteFileSet> readRewritableDeletes(FileIO io, String location) {
    LOG.info("Loading rewritable deletes from {}", location);
    try (ObjectInputStream in = new ObjectInputStream(new GZIPInputStream(io.newInputFile(location).newStream()))) {
      return (Map<String, DeleteFileSet>) in.readObject();
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to read rewritable deletes from %s", location), e);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(String.format("Failed to read rewritable deletes from %s", location), e);
    }
  }

  // keeps the value the object cache logs short
  private record RewritableDeletes(Map<String, DeleteFileSet> deletes) {
    @Override
    public String toString() {
      return "rewritable deletes of " + deletes.size() + " data files";
    }
  }

  public WriterBuilder objectCache(Supplier<ObjectCache> newObjectCache) {
    this.objectCache = newObjectCache;
    return this;
  }

  private boolean shouldRewriteDeletes() {
    // deletes must be rewritten when there are DVs and file-scoped deletes
    return context.useDVs() || context.deleteGranularity() == DeleteGranularity.FILE;
  }

  private static Map<String, DeleteFileSet> rewritableDeletes(BatchScan scan, boolean forDVs) {
    Map<String, DeleteFileSet> rewritableDeletes = Maps.newHashMap();

    try (CloseableIterable<ScanTask> tasksIterable = scan.planFiles()) {
      tasksIterable.forEach(task -> {
        FileScanTask fileScanTask = task.asFileScanTask();

        fileScanTask.deletes().forEach(deleteFile -> {
          if (shouldRewrite(deleteFile, forDVs)) {
            rewritableDeletes
                .computeIfAbsent(fileScanTask.file().location(), ignored -> DeleteFileSet.create())
                .add(deleteFile);
          }
        });
      });
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to close table scan: %s", scan), e);
    }
    return rewritableDeletes;
  }

  // for DVs all position deletes must be rewritten
  // for position deletes, only file-scoped deletes must be rewritten
  private static boolean shouldRewrite(DeleteFile deleteFile, boolean forDVs) {
    if (forDVs) {
      return deleteFile.content() != FileContent.EQUALITY_DELETES;
    }
    return ContentFileUtil.isFileScoped(deleteFile);
  }

  public WriterBuilder addRowLineageColumns(boolean isRowLineage) {
    this.shouldAddRowLineageColumns = isRowLineage;
    return this;
  }

  static class Context {

    private final FileFormat dataFileFormat;
    private final long targetDataFileSize;
    private final FileFormat deleteFileFormat;
    private final long targetDeleteFileSize;
    private final DeleteGranularity deleteGranularity;
    private final boolean useFanoutWriter;
    private final boolean inputOrdered;
    private final boolean isMergeTask;
    private final boolean skipRowData;
    private final boolean useDVs;
    private final Set<String> missingColumns;

    Context(Map<String, String> properties, UnaryOperator<String> ops, String tableName) {
      String dataFileFormatName =
          properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
      this.dataFileFormat = FileFormat.valueOf(dataFileFormatName.toUpperCase(Locale.ENGLISH));

      String deleteFileFormatName =
          properties.getOrDefault(DELETE_DEFAULT_FILE_FORMAT, dataFileFormatName);
      this.deleteFileFormat = FileFormat.valueOf(deleteFileFormatName.toUpperCase(Locale.ENGLISH));

      this.targetDataFileSize = PropertyUtil.propertyAsLong(properties,
          TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
      this.targetDeleteFileSize = PropertyUtil.propertyAsLong(properties,
          TableProperties.DELETE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

      this.inputOrdered = HiveCustomStorageHandlerUtils.getWriteOperationIsSorted(ops, tableName);
      this.useFanoutWriter = !inputOrdered && IcebergTableUtil.isFanoutEnabled(properties);
      this.isMergeTask = HiveCustomStorageHandlerUtils.isMergeTaskEnabled(ops, tableName);

      this.deleteGranularity = DeleteGranularity.PARTITION;
      this.useDVs = IcebergTableUtil.formatVersion(properties) > 2;

      this.skipRowData = useDVs ||
          PropertyUtil.propertyAsBoolean(properties,
            ICEBERG_DELETE_SKIPROWDATA, ICEBERG_DELETE_SKIPROWDATA_DEFAULT);

      this.missingColumns = Optional.ofNullable(ops.apply(SessionStateUtil.MISSING_COLUMNS))
          .map(columns -> Arrays.stream(columns.split(",")).collect(Collectors.toCollection(HashSet::new)))
          .orElse(Sets.newHashSet());
    }

    FileFormat dataFileFormat() {
      return dataFileFormat;
    }

    long targetDataFileSize() {
      return targetDataFileSize;
    }

    FileFormat deleteFileFormat() {
      return deleteFileFormat;
    }

    long targetDeleteFileSize() {
      return targetDeleteFileSize;
    }

    DeleteGranularity deleteGranularity() {
      return deleteGranularity;
    }

    boolean useFanoutWriter() {
      return useFanoutWriter;
    }

    boolean inputOrdered() {
      return inputOrdered;
    }

    boolean isMergeTask() {
      return isMergeTask;
    }

    boolean skipRowData() {
      return skipRowData;
    }

    public boolean useDVs() {
      return useDVs;
    }

    public Set<String> missingColumns() {
      return missingColumns;
    }
  }
}
