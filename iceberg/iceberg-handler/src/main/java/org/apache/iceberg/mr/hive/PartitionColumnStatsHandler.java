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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends the existing partition statistics file with column-level statistics.
 * Adds an optional content_stats struct-of-structs (keyed by field ID) to each partition row,
 * enabling Parquet column projection for efficient reads.
 */
public class PartitionColumnStatsHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionColumnStatsHandler.class);

  static final int NUM_STATS_PER_COLUMN = 200;
  static final int DATA_SPACE_FIELD_ID_START = 10_000;

  static final int LOWER_BOUND_OFFSET = 1;
  static final int UPPER_BOUND_OFFSET = 2;
  static final int NULL_VALUE_COUNT_OFFSET = 3;
  static final int NDV_OFFSET = 4;
  static final int BIT_VECTOR_OFFSET = 5;
  static final int HISTOGRAM_OFFSET = 6;

  static final String CONTENT_STATS_FIELD_NAME = "content_stats";
  static final int CONTENT_STATS_FIELD_ID = 146;

  private PartitionColumnStatsHandler() {
  }

  public static int statsFieldId(int tableFieldId) {
    return DATA_SPACE_FIELD_ID_START + (NUM_STATS_PER_COLUMN * tableFieldId);
  }

  /**
   * Builds the content_stats struct type for the given field IDs.
   */
  public static Types.StructType buildContentStatsType(Schema tableSchema, List<Integer> fieldIds) {
    List<Types.NestedField> contentStatsFields = Lists.newArrayList();
    for (int fieldId : fieldIds) {
      Types.NestedField tableField = tableSchema.findField(fieldId);
      if (tableField == null) {
        continue;
      }
      int baseId = statsFieldId(fieldId);
      Type fieldType = tableField.type();

      List<Types.NestedField> statFields = Lists.newArrayList();
      statFields.add(Types.NestedField.optional(
          baseId + LOWER_BOUND_OFFSET, "lower_bound", fieldType));
      statFields.add(Types.NestedField.optional(
          baseId + UPPER_BOUND_OFFSET, "upper_bound", fieldType));
      statFields.add(Types.NestedField.optional(
          baseId + NULL_VALUE_COUNT_OFFSET, "null_value_count", Types.LongType.get()));
      statFields.add(Types.NestedField.optional(
          baseId + NDV_OFFSET, "ndv", Types.LongType.get()));
      statFields.add(Types.NestedField.optional(
          baseId + BIT_VECTOR_OFFSET, "bit_vector", Types.BinaryType.get()));
      statFields.add(Types.NestedField.optional(
          baseId + HISTOGRAM_OFFSET, "histogram", Types.BinaryType.get()));

      contentStatsFields.add(Types.NestedField.optional(
          baseId, String.valueOf(fieldId), Types.StructType.of(statFields)));
    }
    return Types.StructType.of(contentStatsFields);
  }

  /**
   * Reads the existing partition stats file, extends each row with column stats from Hive,
   * and writes a new combined file replacing the original.
   *
   * @param table the Iceberg table
   * @param snapshotId the snapshot to associate with
   * @param colStatsList per-partition column statistics from Hive
   * @param existingStatsFile the current partition stats file (may be null)
   * @return the new PartitionStatisticsFile reference to register
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static PartitionStatisticsFile writeColumnStatsFile(
      Table table, long snapshotId, List<ColumnStatistics> colStatsList,
      PartitionStatisticsFile existingStatsFile) throws IOException {

    Schema tableSchema = table.schema();
    Types.StructType partitionType = table.spec().partitionType();

    // Collect all field IDs referenced
    List<Integer> fieldIds = colStatsList.stream()
        .flatMap(cs -> cs.getStatsObj().stream())
        .map(obj -> tableSchema.findField(obj.getColName()))
        .filter(java.util.Objects::nonNull)
        .map(Types.NestedField::fieldId)
        .distinct()
        .sorted()
        .collect(java.util.stream.Collectors.toList());

    Types.StructType contentStatsType = buildContentStatsType(tableSchema, fieldIds);

    // Build the extended schema: base partition stats schema + content_stats
    Schema baseSchema = PartitionStatsHandler.schema(partitionType,
        org.apache.iceberg.TableUtil.formatVersion(table));
    List<Types.NestedField> extendedFields = Lists.newArrayList(baseSchema.columns());
    extendedFields.add(Types.NestedField.optional(
        CONTENT_STATS_FIELD_ID, CONTENT_STATS_FIELD_NAME, contentStatsType));
    Schema extendedSchema = new Schema(extendedFields);

    // Index column stats by partition name
    Map<String, List<ColumnStatisticsObj>> colStatsByPartition = Maps.newHashMap();
    Map<Integer, Integer> fieldIdToIndex = Maps.newHashMap();
    for (int i = 0; i < fieldIds.size(); i++) {
      fieldIdToIndex.put(fieldIds.get(i), i);
    }
    for (ColumnStatistics cs : colStatsList) {
      if (!cs.getStatsDesc().isIsTblLevel()) {
        colStatsByPartition.put(cs.getStatsDesc().getPartName(), cs.getStatsObj());
      }
    }

    FileFormat fileFormat = defaultFileFormat(table);
    OutputFile outputFile = newPartitionStatsFile(table, fileFormat, snapshotId);

    try (FileAppender<StructLike> writer =
        InternalData.write(fileFormat, outputFile).schema(extendedSchema).build()) {

      if (existingStatsFile != null) {
        // Read existing rows and extend with content_stats
        try (CloseableIterable<StructLike> existing = InternalData.read(
            FileFormat.fromFileName(existingStatsFile.path()),
            table.io().newInputFile(existingStatsFile.path()))
            .project(baseSchema)
            .build()) {

          for (StructLike baseRecord : existing) {
            GenericRecord extended = GenericRecord.create(extendedSchema.asStruct());
            // Copy base fields
            for (int i = 0; i < baseSchema.columns().size(); i++) {
              extended.set(i, baseRecord.get(i, Object.class));
            }

            // Match partition and add content_stats if we have column stats for it
            String partPath = table.spec().partitionToPath(
                IcebergTableUtil.toPartitionData(
                    baseRecord.get(0, StructLike.class), partitionType, partitionType));
            List<ColumnStatisticsObj> statsObjs = colStatsByPartition.remove(partPath);
            if (statsObjs != null) {
              extended.set(baseSchema.columns().size(),
                  buildContentStats(contentStatsType, statsObjs, tableSchema, fieldIdToIndex));
            }

            writer.add(extended);
          }
        }
      }

      // Write any new partitions not in the existing file
      for (Map.Entry<String, List<ColumnStatisticsObj>> entry : colStatsByPartition.entrySet()) {
        GenericRecord record = GenericRecord.create(extendedSchema.asStruct());
        GenericRecord partRecord = buildPartitionRecord(partitionType, table.spec(),
            entry.getKey());
        record.set(0, partRecord);
        record.set(1, table.spec().specId());
        // Set basic stat fields to 0/null — they'll be populated when basic stats are computed
        record.set(baseSchema.columns().size(),
            buildContentStats(contentStatsType, entry.getValue(), tableSchema, fieldIdToIndex));
        writer.add(record);
      }
    }

    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(snapshotId)
        .path(outputFile.location())
        .fileSizeInBytes(outputFile.toInputFile().getLength())
        .build();
  }

  private static GenericRecord buildContentStats(Types.StructType contentStatsType,
      List<ColumnStatisticsObj> statsObjs, Schema tableSchema,
      Map<Integer, Integer> fieldIdToIndex) {
    GenericRecord contentStats = GenericRecord.create(contentStatsType);
    for (ColumnStatisticsObj statsObj : statsObjs) {
      Types.NestedField field = tableSchema.findField(statsObj.getColName());
      if (field == null) {
        continue;
      }
      Integer idx = fieldIdToIndex.get(field.fieldId());
      if (idx == null) {
        continue;
      }
      int baseId = statsFieldId(field.fieldId());
      Types.StructType statStructType = (Types.StructType)
          contentStatsType.field(baseId).type();
      GenericRecord statRecord = GenericRecord.create(statStructType);
      populateStatRecord(statRecord, statsObj, field.type());
      contentStats.set(idx, statRecord);
    }
    return contentStats;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static void populateStatRecord(
      GenericRecord statRecord, ColumnStatisticsObj statsObj, Type icebergType) {
    ColumnStatisticsData data = statsObj.getStatsData();

    Object lowValue = null;
    Object highValue = null;
    long numNulls = 0;
    long numDVs = 0;
    byte[] bitVectors = null;
    byte[] histogram = null;

    if (data.isSetLongStats()) {
      LongColumnStatsData sd = data.getLongStats();
      lowValue = sd.isSetLowValue() ? sd.getLowValue() : null;
      highValue = sd.isSetHighValue() ? sd.getHighValue() : null;
      numNulls = sd.getNumNulls();
      numDVs = sd.getNumDVs();
      bitVectors = sd.getBitVectors();
      histogram = sd.getHistogram();
    } else if (data.isSetDoubleStats()) {
      DoubleColumnStatsData sd = data.getDoubleStats();
      lowValue = sd.isSetLowValue() ? sd.getLowValue() : null;
      highValue = sd.isSetHighValue() ? sd.getHighValue() : null;
      numNulls = sd.getNumNulls();
      numDVs = sd.getNumDVs();
      bitVectors = sd.getBitVectors();
      histogram = sd.getHistogram();
    } else if (data.isSetStringStats()) {
      StringColumnStatsData sd = data.getStringStats();
      numNulls = sd.getNumNulls();
      numDVs = sd.getNumDVs();
      bitVectors = sd.getBitVectors();
    } else if (data.isSetDecimalStats()) {
      DecimalColumnStatsData sd = data.getDecimalStats();
      lowValue = sd.isSetLowValue() ?
          DecimalUtils.getHiveDecimal(sd.getLowValue()).bigDecimalValue() : null;
      highValue = sd.isSetHighValue() ?
          DecimalUtils.getHiveDecimal(sd.getHighValue()).bigDecimalValue() : null;
      numNulls = sd.getNumNulls();
      numDVs = sd.getNumDVs();
      bitVectors = sd.getBitVectors();
      histogram = sd.getHistogram();
    } else if (data.isSetDateStats()) {
      DateColumnStatsData sd = data.getDateStats();
      lowValue = sd.isSetLowValue() ? (int) sd.getLowValue().getDaysSinceEpoch() : null;
      highValue = sd.isSetHighValue() ? (int) sd.getHighValue().getDaysSinceEpoch() : null;
      numNulls = sd.getNumNulls();
      numDVs = sd.getNumDVs();
      bitVectors = sd.getBitVectors();
      histogram = sd.getHistogram();
    } else if (data.isSetTimestampStats()) {
      TimestampColumnStatsData sd = data.getTimestampStats();
      lowValue = sd.isSetLowValue() ?
          sd.getLowValue().getSecondsSinceEpoch() * 1_000_000L : null;
      highValue = sd.isSetHighValue() ?
          sd.getHighValue().getSecondsSinceEpoch() * 1_000_000L : null;
      numNulls = sd.getNumNulls();
      numDVs = sd.getNumDVs();
      bitVectors = sd.getBitVectors();
      histogram = sd.getHistogram();
    } else if (data.isSetBooleanStats()) {
      BooleanColumnStatsData sd = data.getBooleanStats();
      numNulls = sd.getNumNulls();
      bitVectors = sd.getBitVectors();
    } else if (data.isSetBinaryStats()) {
      BinaryColumnStatsData sd = data.getBinaryStats();
      numNulls = sd.getNumNulls();
      bitVectors = sd.getBitVectors();
    }

    writeStatFields(statRecord, lowValue, highValue, numNulls, numDVs, bitVectors, histogram,
        icebergType);
  }

  private static void writeStatFields(GenericRecord statRecord, Object lowValue, Object highValue,
      long numNulls, long numDVs, byte[] bitVectors, byte[] histogram, Type icebergType) {
    if (lowValue != null) {
      statRecord.set(0, coerceToIcebergType(lowValue, icebergType));
    }
    if (highValue != null) {
      statRecord.set(1, coerceToIcebergType(highValue, icebergType));
    }
    statRecord.set(2, numNulls);
    if (numDVs > 0) {
      statRecord.set(3, numDVs);
    }
    if (bitVectors != null && bitVectors.length > 0) {
      statRecord.set(4, ByteBuffer.wrap(bitVectors));
    }
    if (histogram != null && histogram.length > 0) {
      statRecord.set(5, ByteBuffer.wrap(histogram));
    }
  }

  private static Object coerceToIcebergType(Object value, Type icebergType) {
    if (value == null) {
      return null;
    }
    switch (icebergType.typeId()) {
      case INTEGER:
        return ((Number) value).intValue();
      case LONG:
        return ((Number) value).longValue();
      case FLOAT:
        return ((Number) value).floatValue();
      case DOUBLE:
        return ((Number) value).doubleValue();
      default:
        return value;
    }
  }

  /**
   * Reads column stats from the partition stats file with projection on requested columns.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static List<ColumnStatistics> readAsColumnStatistics(
      Table table, long snapshotId, List<String> colNames, Set<String> partNames) {

    PartitionStatisticsFile statsFile = IcebergTableUtil.getPartitionStatsFile(table, snapshotId);
    if (statsFile == null) {
      return Lists.newArrayList();
    }

    Schema tableSchema = table.schema();
    Types.StructType partitionType = table.spec().partitionType();

    List<Integer> fieldIds = null;
    if (colNames != null) {
      fieldIds = colNames.stream()
          .map(tableSchema::findField)
          .filter(java.util.Objects::nonNull)
          .map(Types.NestedField::fieldId)
          .collect(java.util.stream.Collectors.toList());
    }

    // Build projected schema: partition_data + spec_id + content_stats (projected fields only)
    List<Integer> projectedFieldIds = fieldIds != null ? fieldIds :
        tableSchema.columns().stream()
            .map(Types.NestedField::fieldId)
            .collect(java.util.stream.Collectors.toList());

    Types.StructType contentStatsType = buildContentStatsType(tableSchema, projectedFieldIds);
    List<Types.NestedField> projectedFields = Lists.newArrayList();
    projectedFields.add(Types.NestedField.required(
        PartitionStatsHandler.PARTITION_FIELD_ID, PartitionStatsHandler.PARTITION_FIELD_NAME,
        partitionType));
    projectedFields.add(PartitionStatsHandler.SPEC_ID);
    projectedFields.add(Types.NestedField.optional(
        CONTENT_STATS_FIELD_ID, CONTENT_STATS_FIELD_NAME, contentStatsType));
    Schema projectedSchema = new Schema(projectedFields);

    // Pre-parse partition names for efficient filtering
    Map<String, Map<String, String>> parsedPartNames = null;
    if (partNames != null) {
      parsedPartNames = Maps.newHashMapWithExpectedSize(partNames.size());
      for (String pn : partNames) {
        try {
          parsedPartNames.put(pn,
              org.apache.hadoop.hive.metastore.Warehouse.makeSpecFromName(pn));
        } catch (org.apache.hadoop.hive.metastore.api.MetaException e) {
          LOG.warn("Failed to parse partition name: {}", pn);
        }
      }
    }

    List<ColumnStatistics> result = Lists.newArrayList();
    List<Types.NestedField> partFields = partitionType.fields();

    try (CloseableIterable<StructLike> records = InternalData.read(
        FileFormat.fromFileName(statsFile.path()),
        table.io().newInputFile(statsFile.path()))
        .project(projectedSchema)
        .build()) {

      for (StructLike record : records) {
        StructLike partData = record.get(0, StructLike.class);

        String matchedPartName;
        if (parsedPartNames != null) {
          matchedPartName = matchPartition(partData, partFields, parsedPartNames);
          if (matchedPartName == null) {
            continue;
          }
        } else {
          matchedPartName = table.spec().partitionToPath(
              IcebergTableUtil.toPartitionData(partData, partitionType, partitionType));
        }

        StructLike contentStats = record.get(2, StructLike.class);
        if (contentStats == null) {
          continue;
        }

        List<ColumnStatisticsObj> statsObjs = Lists.newArrayList();
        for (int idx = 0; idx < projectedFieldIds.size(); idx++) {
          int fieldId = projectedFieldIds.get(idx);
          StructLike fieldStats = contentStats.get(idx, StructLike.class);
          if (fieldStats == null) {
            continue;
          }
          Types.NestedField tableField = tableSchema.findField(fieldId);
          ColumnStatisticsObj statsObj = recordToColumnStatisticsObj(tableField, fieldStats);
          if (statsObj != null) {
            statsObjs.add(statsObj);
          }
        }

        if (!statsObjs.isEmpty()) {
          org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc desc =
              new org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc(false, null, null);
          desc.setPartName(matchedPartName);
          result.add(new ColumnStatistics(desc, statsObjs));
        }
      }
    } catch (IOException e) {
      LOG.warn("Unable to read partition column stats: {}", e.getMessage());
    }

    return result;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static ColumnStatisticsObj recordToColumnStatisticsObj(
      Types.NestedField tableField, StructLike fieldStats) {
    String colName = tableField.name();
    Long ndv = fieldStats.get(3, Long.class);
    Long nullCount = fieldStats.get(2, Long.class);

    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName(colName);
    obj.setColType(org.apache.iceberg.hive.HiveSchemaUtil.convertToTypeString(tableField.type()));

    ColumnStatisticsData statsData = new ColumnStatisticsData();
    Type.TypeID typeId = tableField.type().typeId();

    switch (typeId) {
      case INTEGER:
      case LONG:
        LongColumnStatsData longData = new LongColumnStatsData();
        longData.setNumNulls(nullCount != null ? nullCount : 0);
        longData.setNumDVs(ndv != null ? ndv : 0);
        Object lowLong = fieldStats.get(0, Object.class);
        Object highLong = fieldStats.get(1, Object.class);
        if (lowLong instanceof Number) {
          longData.setLowValue(((Number) lowLong).longValue());
        }
        if (highLong instanceof Number) {
          longData.setHighValue(((Number) highLong).longValue());
        }
        ByteBuffer bvLong = fieldStats.get(4, ByteBuffer.class);
        if (bvLong != null) {
          longData.setBitVectors(toByteArray(bvLong));
        }
        statsData.setLongStats(longData);
        break;
      case FLOAT:
      case DOUBLE:
        DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
        doubleData.setNumNulls(nullCount != null ? nullCount : 0);
        doubleData.setNumDVs(ndv != null ? ndv : 0);
        Object lowDouble = fieldStats.get(0, Object.class);
        Object highDouble = fieldStats.get(1, Object.class);
        if (lowDouble instanceof Number) {
          doubleData.setLowValue(((Number) lowDouble).doubleValue());
        }
        if (highDouble instanceof Number) {
          doubleData.setHighValue(((Number) highDouble).doubleValue());
        }
        ByteBuffer bvDouble = fieldStats.get(4, ByteBuffer.class);
        if (bvDouble != null) {
          doubleData.setBitVectors(toByteArray(bvDouble));
        }
        statsData.setDoubleStats(doubleData);
        break;
      case STRING:
        StringColumnStatsData stringData = new StringColumnStatsData();
        stringData.setNumNulls(nullCount != null ? nullCount : 0);
        stringData.setNumDVs(ndv != null ? ndv : 0);
        ByteBuffer bvString = fieldStats.get(4, ByteBuffer.class);
        if (bvString != null) {
          stringData.setBitVectors(toByteArray(bvString));
        }
        statsData.setStringStats(stringData);
        break;
      case DATE:
        DateColumnStatsData dateData = new DateColumnStatsData();
        dateData.setNumNulls(nullCount != null ? nullCount : 0);
        dateData.setNumDVs(ndv != null ? ndv : 0);
        Object lowDate = fieldStats.get(0, Object.class);
        Object highDate = fieldStats.get(1, Object.class);
        if (lowDate instanceof Integer) {
          dateData.setLowValue(
              new org.apache.hadoop.hive.metastore.api.Date((Integer) lowDate));
        }
        if (highDate instanceof Integer) {
          dateData.setHighValue(
              new org.apache.hadoop.hive.metastore.api.Date((Integer) highDate));
        }
        ByteBuffer bvDate = fieldStats.get(4, ByteBuffer.class);
        if (bvDate != null) {
          dateData.setBitVectors(toByteArray(bvDate));
        }
        statsData.setDateStats(dateData);
        break;
      case TIMESTAMP:
        TimestampColumnStatsData tsData = new TimestampColumnStatsData();
        tsData.setNumNulls(nullCount != null ? nullCount : 0);
        tsData.setNumDVs(ndv != null ? ndv : 0);
        Object lowTs = fieldStats.get(0, Object.class);
        Object highTs = fieldStats.get(1, Object.class);
        if (lowTs instanceof Long) {
          tsData.setLowValue(
              new org.apache.hadoop.hive.metastore.api.Timestamp((Long) lowTs / 1_000_000L));
        }
        if (highTs instanceof Long) {
          tsData.setHighValue(
              new org.apache.hadoop.hive.metastore.api.Timestamp((Long) highTs / 1_000_000L));
        }
        ByteBuffer bvTs = fieldStats.get(4, ByteBuffer.class);
        if (bvTs != null) {
          tsData.setBitVectors(toByteArray(bvTs));
        }
        statsData.setTimestampStats(tsData);
        break;
      case DECIMAL:
        DecimalColumnStatsData decData = new DecimalColumnStatsData();
        decData.setNumNulls(nullCount != null ? nullCount : 0);
        decData.setNumDVs(ndv != null ? ndv : 0);
        Object lowDec = fieldStats.get(0, Object.class);
        Object highDec = fieldStats.get(1, Object.class);
        if (lowDec instanceof java.math.BigDecimal) {
          decData.setLowValue(DecimalUtils.createThriftDecimal(lowDec.toString()));
        }
        if (highDec instanceof java.math.BigDecimal) {
          decData.setHighValue(DecimalUtils.createThriftDecimal(highDec.toString()));
        }
        ByteBuffer bvDec = fieldStats.get(4, ByteBuffer.class);
        if (bvDec != null) {
          decData.setBitVectors(toByteArray(bvDec));
        }
        statsData.setDecimalStats(decData);
        break;
      case BOOLEAN:
        BooleanColumnStatsData boolData = new BooleanColumnStatsData();
        boolData.setNumNulls(nullCount != null ? nullCount : 0);
        ByteBuffer bvBool = fieldStats.get(4, ByteBuffer.class);
        if (bvBool != null) {
          boolData.setBitVectors(toByteArray(bvBool));
        }
        statsData.setBooleanStats(boolData);
        break;
      case BINARY:
      case FIXED:
        BinaryColumnStatsData binData = new BinaryColumnStatsData();
        binData.setNumNulls(nullCount != null ? nullCount : 0);
        ByteBuffer bvBin = fieldStats.get(4, ByteBuffer.class);
        if (bvBin != null) {
          binData.setBitVectors(toByteArray(bvBin));
        }
        statsData.setBinaryStats(binData);
        break;
      default:
        LOG.debug("Unsupported type for column stats: {}", typeId);
        return null;
    }

    obj.setStatsData(statsData);
    return obj;
  }

  private static String matchPartition(StructLike partData, List<Types.NestedField> partFields,
      Map<String, Map<String, String>> parsedPartNames) {
    for (Map.Entry<String, Map<String, String>> entry : parsedPartNames.entrySet()) {
      Map<String, String> expectedValues = entry.getValue();
      boolean matches = true;
      for (int i = 0; i < partFields.size(); i++) {
        String fieldName = partFields.get(i).name();
        String expectedValue = expectedValues.get(fieldName);
        if (expectedValue == null) {
          continue;
        }
        Object actualValue = partData.get(i, Object.class);
        String actualStr = actualValue == null ? null : String.valueOf(actualValue);
        if (!expectedValue.equals(actualStr)) {
          matches = false;
          break;
        }
      }
      if (matches) {
        return entry.getKey();
      }
    }
    return null;
  }

  private static GenericRecord buildPartitionRecord(
      Types.StructType partitionType, PartitionSpec spec, String partName) {
    GenericRecord partRecord = GenericRecord.create(partitionType);
    Map<String, String> partValues;
    try {
      partValues = org.apache.hadoop.hive.metastore.Warehouse.makeSpecFromName(partName);
    } catch (org.apache.hadoop.hive.metastore.api.MetaException e) {
      throw new RuntimeException("Failed to parse partition name: " + partName, e);
    }
    List<Types.NestedField> fields = partitionType.fields();
    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField field = fields.get(i);
      String value = partValues.get(field.name());
      if (value != null) {
        partRecord.set(i,
            org.apache.iceberg.types.Conversions.fromPartitionString(field.type(), value));
      }
    }
    return partRecord;
  }

  private static byte[] toByteArray(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  private static FileFormat defaultFileFormat(Table table) {
    return FileFormat.fromString(
        table.properties().getOrDefault("write.format.default", "parquet"));
  }

  private static OutputFile newPartitionStatsFile(Table table, FileFormat fileFormat,
      long snapshotId) {
    return table.io().newOutputFile(
        ((HasTableOperations) table).operations().metadataFileLocation(
            fileFormat.addExtension(
                String.format(Locale.ROOT, "partition-stats-%d-%s",
                    snapshotId, UUID.randomUUID()))));
  }
}
