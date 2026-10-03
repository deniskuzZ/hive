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
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergTestUtils;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Tests Format specific features, such as reading/writing tables, using delete files, etc.
 */
public class TestHiveIcebergCRUD extends HiveIcebergStorageHandlerWithEngineBase {

  @Test
  public void testReadAndWriteFormatV2UnpartitionedWithEqDelete() throws IOException {
    Assume.assumeTrue("Reading V2 tables with eq delete files are only supported currently in " +
        "non-vectorized mode", !isVectorized && formatVersion == 2);

    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // delete one of the rows
    List<Record> toDelete = TestHelper.RecordsBuilder
        .newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).add(1L, "Bob", null).build();
    DeleteFile deleteFile = HiveIcebergTestUtils.createEqualityDeleteFile(tbl, "dummyPath",
        ImmutableList.of("customer_id", "first_name"), fileFormat, toDelete);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id");

    // only the other two rows are present
    Assert.assertEquals(2, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, objects.get(1));
  }

  @Test
  public void testReadAndWriteFormatV2Partitioned_EqDelete_AllColumnsSupplied() throws IOException {
    Assume.assumeTrue("Reading V2 tables with eq delete files are only supported currently in " +
        "non-vectorized mode", !isVectorized && formatVersion == 2);

    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("customer_id").build();
    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // add one more row to the same partition
    shell.executeStatement("insert into customers values (1, 'Bob', 'Hoover')");

    // delete all rows with id=1 and first_name=Bob
    List<Record> toDelete = TestHelper.RecordsBuilder
        .newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).add(1L, "Bob", null).build();
    DeleteFile deleteFile = HiveIcebergTestUtils.createEqualityDeleteFile(tbl, "dummyPath",
        ImmutableList.of("customer_id", "first_name"), fileFormat, toDelete);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id");

    Assert.assertEquals(2, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, objects.get(1));
  }

  @Test
  public void testReadAndWriteFormatV2Partitioned_EqDelete_OnlyEqColumnsSupplied() throws IOException {
    Assume.assumeTrue("Reading V2 tables with eq delete files are only supported currently in " +
        "non-vectorized mode", !isVectorized && formatVersion == 2);

    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("customer_id").build();
    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // add one more row to the same partition
    shell.executeStatement("insert into customers values (1, 'Bob', 'Hoover')");

    // delete all rows with id=1 and first_name=Bob
    Schema shorterSchema = new Schema(
        optional(1, "id", Types.LongType.get()), optional(2, "name", Types.StringType.get()));
    List<Record> toDelete = TestHelper.RecordsBuilder.newInstance(shorterSchema).add(1L, "Bob").build();
    DeleteFile deleteFile = HiveIcebergTestUtils.createEqualityDeleteFile(tbl, "dummyPath",
        ImmutableList.of("customer_id", "first_name"), fileFormat, toDelete);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id");

    Assert.assertEquals(2, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, objects.get(1));
  }

  @Test
  public void testReadAndWriteFormatV2Unpartitioned_PosDelete() throws IOException {
    Assume.assumeTrue(formatVersion == 2);

    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // delete one of the rows
    DataFile dataFile = StreamSupport.stream(tbl.currentSnapshot().addedDataFiles(tbl.io()).spliterator(), false)
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Did not find any data files for test table"));
    List<PositionDelete<Record>> deletes = ImmutableList.of(positionDelete(
        dataFile.path(), 2L, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(2))
    );
    DeleteFile deleteFile = HiveIcebergTestUtils.createPositionalDeleteFile(tbl, "dummyPath",
        fileFormat, null, deletes);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id");

    // only the other two rows are present
    Assert.assertEquals(2, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, objects.get(1));
  }

  @Test
  public void testReadAndWriteFormatV2Partitioned_PosDelete_RowNotSupplied() throws IOException {
    Assume.assumeTrue(formatVersion == 2);

    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("customer_id").build();
    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // add some more data to the same partition
    shell.executeStatement("insert into customers values (0, 'Laura', 'Yellow'), (0, 'John', 'Green'), " +
        "(0, 'Blake', 'Blue')");
    tbl.refresh();

    // delete the first and third rows from the newly-added data file - with row supplied
    DataFile dataFile = StreamSupport.stream(tbl.currentSnapshot().addedDataFiles(tbl.io()).spliterator(), false)
        .filter(file -> file.partition().get(0, Long.class) == 0L)
        .filter(file -> file.recordCount() == 3)
        .findAny()
        .orElseThrow(() -> new RuntimeException("Did not find the desired data file in the test table"));
    List<PositionDelete<Record>> deletes = ImmutableList.of(
        positionDelete(dataFile.path(), 0L, null),
        positionDelete(dataFile.path(), 2L, null)
    );
    DeleteFile deleteFile = HiveIcebergTestUtils.createPositionalDeleteFile(tbl, "dummyPath",
        fileFormat, ImmutableMap.of("customer_id", 0L), deletes);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, first_name");

    Assert.assertEquals(4, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {0L, "John", "Green"}, objects.get(1));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, objects.get(2));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, objects.get(3));
  }

  @Test
  public void testReadAndWriteFormatV2Partitioned_PosDelete_RowSupplied() throws IOException {
    Assume.assumeTrue(formatVersion == 2);

    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("customer_id").build();
    Table tbl = testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, formatVersion);

    // add some more data to the same partition
    shell.executeStatement("insert into customers values (0, 'Laura', 'Yellow'), (0, 'John', 'Green'), " +
        "(0, 'Blake', 'Blue')");
    tbl.refresh();

    // delete the first and third rows from the newly-added data file
    DataFile dataFile = StreamSupport.stream(tbl.currentSnapshot().addedDataFiles(tbl.io()).spliterator(), false)
        .filter(file -> file.partition().get(0, Long.class) == 0L)
        .filter(file -> file.recordCount() == 3)
        .findAny()
        .orElseThrow(() -> new RuntimeException("Did not find the desired data file in the test table"));
    List<Record> rowsToDel = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(0L, "Laura", "Yellow").add(0L, "Blake", "Blue").build();
    List<PositionDelete<Record>> deletes = ImmutableList.of(
        positionDelete(dataFile.path(), 0L, rowsToDel.get(0)),
        positionDelete(dataFile.path(), 2L, rowsToDel.get(1))
    );
    DeleteFile deleteFile = HiveIcebergTestUtils.createPositionalDeleteFile(tbl, "dummyPath",
        fileFormat, ImmutableMap.of("customer_id", 0L), deletes);
    tbl.newRowDelta().addDeletes(deleteFile).commit();

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, first_name");

    Assert.assertEquals(4, objects.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, objects.get(0));
    Assert.assertArrayEquals(new Object[] {0L, "John", "Green"}, objects.get(1));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, objects.get(2));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, objects.get(3));
  }

  @Test
  public void testDeleteStatementUnpartitioned() throws TException, InterruptedException {
    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion);

    // verify delete mode set to merge-on-read
    if (formatVersion == 2) {
      Assert.assertEquals(HiveIcebergStorageHandler.MERGE_ON_READ,
          shell.metastore().getTable("default", "customers")
          .getParameters().get(TableProperties.DELETE_MODE));
    }

    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    shell.executeStatement("DELETE FROM customers WHERE customer_id=3 or first_name='Joanna'");

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name");
    Assert.assertEquals(6, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Bob", "Silver")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testDeleteStatementPartitioned() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    shell.executeStatement("DELETE FROM customers WHERE customer_id=3 or first_name='Joanna'");

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name");
    Assert.assertEquals(6, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Bob", "Silver")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testDeleteStatementWithOtherTable() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create a couple of tables, with an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    testTables.createTable(shell, "other", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1, formatVersion);

    shell.executeStatement("DELETE FROM customers WHERE customer_id in (select t1.customer_id from customers t1 join " +
        "other t2 on t1.customer_id = t2.customer_id) or " +
        "first_name in (select first_name from customers where first_name = 'Bob')");

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name");
    Assert.assertEquals(5, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Pierce")
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Joanna", "Silver")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testDeleteStatementWithPartitionAndSchemaEvolution() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    // change the partition spec + schema, and insert some new records
    shell.executeStatement("ALTER TABLE customers SET PARTITION SPEC (bucket(64, last_name))");
    shell.executeStatement("ALTER TABLE customers ADD COLUMNS (department string)");
    shell.executeStatement("ALTER TABLE customers CHANGE COLUMN first_name given_name string first");
    shell.executeStatement("INSERT INTO customers VALUES ('Natalie', 20, 'Bloom', 'Finance'), ('Joanna', 22, " +
        "'Huberman', 'Operations')");

    // the delete should handle deleting records both from older specs and from the new spec without problems
    // there are records with Joanna in both the old and new spec, as well as the old and new schema
    shell.executeStatement("DELETE FROM customers WHERE customer_id=3 or given_name='Joanna'");

    List<Object[]> objects = shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name");
    Assert.assertEquals(7, objects.size());
    // partition-filtered reads of files of the old spec find the deletes, which carry the partitions of that spec
    Assert.assertEquals(0L, shell.executeStatement(
        "SELECT count(*) FROM customers WHERE last_name IN ('Barna', 'Burr', 'Pierce', 'Silver') " +
        "AND (customer_id = 3 OR given_name = 'Joanna')").get(0)[0]);

    Schema newSchema = new Schema(
        optional(2, "given_name", Types.StringType.get()),
        optional(1, "customer_id", Types.LongType.get()),
        optional(3, "last_name", Types.StringType.get(), "This is last name"),
        optional(4, "department", Types.StringType.get())
    );
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(newSchema)
        .add("Sharon", 1L, "Taylor", null)
        .add("Jake", 2L, "Donnel", null)
        .add("Susan", 2L, "Morrison", null)
        .add("Bob", 2L, "Silver", null)
        .add("Laci", 4L, "Zold", null)
        .add("Peti", 5L, "Rozsaszin", null)
        .add("Natalie", 20L, "Bloom", "Finance")
        .build();
    HiveIcebergTestUtils.validateData(expected, HiveIcebergTestUtils.valueForRow(newSchema, objects), 0);
  }

  @Test
  public void testDeleteForSupportedTypes() throws IOException {
    Assume.assumeTrue(formatVersion == 2);

    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);

      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() &&
            (fileFormat == FileFormat.PARQUET || fileFormat == FileFormat.ORC && isVectorized) ||
          type == Types.TimeType.get() &&
            fileFormat == FileFormat.PARQUET  && isVectorized) {
        continue;
      }

      // TODO: remove this filter when we figure out how we could test binary types
      if (type == Types.BinaryType.get() || type.equals(Types.FixedType.ofLength(5))) {
        continue;
      }

      String tableName = type.typeId().toString().toLowerCase() + "_table_" + i;
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, columnName, type));
      List<Record> records = TestHelper.generateRandomRecords(schema, 1, 0L);
      Table table = testTables.createTable(shell, tableName, schema, PartitionSpec.unpartitioned(), fileFormat, records,
          formatVersion);

      shell.executeStatement("DELETE FROM " + tableName);
      HiveIcebergTestUtils.validateData(table, ImmutableList.of(), 0);
    }
  }

  @Test
  public void testUpdateStatementUnpartitioned() throws TException, InterruptedException {
    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2,
        formatVersion);

    // verify update mode set to merge-on-read
    if (formatVersion == 2) {
      Assert.assertEquals(HiveIcebergStorageHandler.MERGE_ON_READ,
          shell.metastore().getTable("default", "customers")
          .getParameters().get(TableProperties.UPDATE_MODE));
    }

    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    shell.executeStatement("UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'");

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, first_name");
    Assert.assertEquals(12, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Changed")
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Joanna", "Changed")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Bob", "Silver")
        .add(3L, "Blake", "Changed")
        .add(3L, "Marci", "Changed")
        .add(3L, "Trudy", "Changed")
        .add(3L, "Trudy", "Changed")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testUpdateStatementPartitioned() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    shell.executeStatement("UPDATE customers SET last_name='Changed' WHERE customer_id=3 or first_name='Joanna'");

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, first_name");
    Assert.assertEquals(12, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Changed")
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Joanna", "Changed")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Bob", "Silver")
        .add(3L, "Blake", "Changed")
        .add(3L, "Marci", "Changed")
        .add(3L, "Trudy", "Changed")
        .add(3L, "Trudy", "Changed")
        .add(4L, "Laci", "Zold")
        .add(5L, "Peti", "Rozsaszin")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testUpdateStatementWithOtherTable() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create a couple of tables, with an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    testTables.createTable(shell, "other", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1, formatVersion);

    shell.executeStatement("UPDATE customers SET last_name='Changed' WHERE customer_id in " +
        "(select t1.customer_id from customers t1 join other t2 on t1.customer_id = t2.customer_id) or " +
        "first_name in (select first_name from customers where first_name = 'Bob')");

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, last_name");
    Assert.assertEquals(9, objects.size());
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .add(1L, "Joanna", "Pierce")
        .add(1L, "Sharon", "Taylor")
        .add(2L, "Bob", "Changed")
        .add(2L, "Jake", "Donnel")
        .add(2L, "Susan", "Morrison")
        .add(2L, "Joanna", "Silver")
        .add(3L, "Blake", "Changed")
        .add(3L, "Trudy", "Changed")
        .add(3L, "Trudy", "Changed")
        .build();
    HiveIcebergTestUtils.validateData(expected,
        HiveIcebergTestUtils.valueForRow(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, objects), 0);
  }

  @Test
  public void testUpdateStatementWithPartitionAndSchemaEvolution() {
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("last_name").bucket("customer_id", 16).build();

    // create and insert an initial batch of records
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec, fileFormat, HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_2, formatVersion);
    // insert one more batch so that we have multiple data files within the same partition
    shell.executeStatement(testTables.getInsertQuery(HiveIcebergStorageHandlerTestUtils.OTHER_CUSTOMER_RECORDS_1,
        TableIdentifier.of("default", "customers"), false));

    // change the partition spec + schema, and insert some new records
    shell.executeStatement("ALTER TABLE customers SET PARTITION SPEC (bucket(64, last_name))");
    shell.executeStatement("ALTER TABLE customers ADD COLUMNS (department string)");
    shell.executeStatement("ALTER TABLE customers CHANGE COLUMN first_name given_name string first");
    shell.executeStatement("INSERT INTO customers VALUES ('Natalie', 20, 'Bloom', 'Finance'), ('Joanna', 22, " +
        "'Huberman', 'Operations')");

    // update should handle changing records both from older specs and from the new spec without problems
    // there are records with Joanna in both the old and new spec, as well as the old and new schema
    shell.executeStatement("UPDATE customers set last_name='Changed' WHERE customer_id=3 or given_name='Joanna'");

    List<Object[]> objects =
        shell.executeStatement("SELECT * FROM customers ORDER BY customer_id, last_name, given_name");
    Assert.assertEquals(14, objects.size());

    Schema newSchema = new Schema(
        optional(2, "given_name", Types.StringType.get()),
        optional(1, "customer_id", Types.LongType.get()),
        optional(3, "last_name", Types.StringType.get(), "This is last name"),
        optional(4, "department", Types.StringType.get())
    );
    List<Record> expected = TestHelper.RecordsBuilder.newInstance(newSchema)
        .add("Joanna", 1L, "Changed", null)
        .add("Sharon", 1L, "Taylor", null)
        .add("Joanna", 2L, "Changed", null)
        .add("Jake", 2L, "Donnel", null)
        .add("Susan", 2L, "Morrison", null)
        .add("Bob", 2L, "Silver", null)
        .add("Blake", 3L, "Changed", null)
        .add("Marci", 3L, "Changed", null)
        .add("Trudy", 3L, "Changed", null)
        .add("Trudy", 3L, "Changed", null)
        .add("Laci", 4L, "Zold", null)
        .add("Peti", 5L, "Rozsaszin", null)
        .add("Natalie", 20L, "Bloom", "Finance")
        .add("Joanna", 22L, "Changed", "Operations")
        .build();
    HiveIcebergTestUtils.validateData(expected, HiveIcebergTestUtils.valueForRow(newSchema, objects), 0);
  }

  @Test
  public void testDeleteFromFilesOfSpecWithDroppedSource() throws IOException {
    Assume.assumeTrue(formatVersion >= 2 && testTableType == TestTableType.HIVE_CATALOG);
    // a Hive query reads the Iceberg partition statistics, which fail once a partition source is dropped
    shell.setHiveSessionValue("hive.iceberg.stats.source", "metastore");
    shell.executeStatement(String.format("CREATE EXTERNAL TABLE dropped (id int, name string, region string) " +
        "PARTITIONED BY SPEC (region) STORED BY ICEBERG STORED AS %s TBLPROPERTIES ('format-version'='%d')",
        fileFormat, formatVersion));
    shell.executeStatement("INSERT INTO dropped VALUES (1, 'a', 'eu'), (2, 'b', 'us'), (3, 'c', 'eu'), (4, 'd', 'us')");
    shell.executeStatement("ALTER TABLE dropped SET PARTITION SPEC (bucket(2, id))");
    shell.executeStatement("INSERT INTO dropped VALUES (5, 'e', 'eu'), (6, 'f', 'us')");
    shell.executeStatement("ALTER TABLE dropped DROP COLUMN region");
    shell.executeStatement("CREATE EXTERNAL TABLE dropped_src (id int) STORED BY ICEBERG");
    shell.executeStatement("INSERT INTO dropped_src VALUES (3), (4), (7)");

    // the statements delete from files of both specs
    if (formatVersion == 2) {
      IllegalArgumentException failure = Assert.assertThrows(IllegalArgumentException.class,
          () -> shell.executeStatement("DELETE FROM dropped WHERE id IN (1, 5)"));
      Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(
          "Cannot delete from the data files of partition spec 0: the source column of a partition field was dropped"));
      Assert.assertEquals(List.of("1:a", "2:b", "3:c", "4:d", "5:e", "6:f"),
          rows(testTables.loadTable(TableIdentifier.of("default", "dropped"))));
      return;
    }
    shell.executeStatement("DELETE FROM dropped WHERE id IN (1, 5)");
    shell.executeStatement("UPDATE dropped SET name = 'upd' WHERE id IN (2, 6)");
    shell.executeStatement("MERGE INTO dropped t USING dropped_src s ON t.id = s.id " +
        "WHEN MATCHED AND t.id = 3 THEN UPDATE SET name = 'mrg' WHEN MATCHED THEN DELETE " +
        "WHEN NOT MATCHED THEN INSERT VALUES (s.id, 'new')");

    Table table = testTables.loadTable(TableIdentifier.of("default", "dropped"));
    Assert.assertEquals(List.of("2:upd", "3:mrg", "6:upd", "7:new"), rows(table));
    // a DV holds the partition of its data file, but for the field of the dropped source, which reads as null
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        for (DeleteFile dv : task.deletes()) {
          Assert.assertEquals(task.file().specId(), dv.specId());
          List<Types.NestedField> fields = task.spec().partitionType().fields();
          for (int pos = 0; pos < fields.size(); pos++) {
            Object expected = fields.get(pos).type().typeId() == Type.TypeID.UNKNOWN ? null :
                task.file().partition().get(pos, Object.class);
            Assert.assertEquals(expected, dv.partition().get(pos, Object.class));
          }
        }
      }
    }
  }

  // reads with Iceberg: Hive fails to plan a query of a table whose partition source was dropped
  private static List<String> rows(Table table) throws IOException {
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      return StreamSupport.stream(records.spliterator(), false)
          .map(row -> row.getField("id") + ":" + row.getField("name"))
          .sorted()
          .toList();
    }
  }

  @Test
  public void testUpdateForSupportedTypes() throws IOException {
    Assume.assumeTrue(formatVersion >= 2);

    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);

      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() &&
            (fileFormat == FileFormat.PARQUET || fileFormat == FileFormat.ORC && isVectorized) ||
          type == Types.TimeType.get() &&
            fileFormat == FileFormat.PARQUET  && isVectorized) {
        continue;
      }

      // TODO: remove this filter when we figure out how we could test binary types
      if (type == Types.BinaryType.get() || type.equals(Types.FixedType.ofLength(5))) {
        continue;
      }

      String tableName = type.typeId().toString().toLowerCase() + "_table_" + i;
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, columnName, type));
      List<Record> originalRecords = TestHelper.generateRandomRecords(schema, 1, 0L);
      Table table = testTables.createTable(shell, tableName, schema, PartitionSpec.unpartitioned(), fileFormat,
          originalRecords, formatVersion);

      List<Record> newRecords = TestHelper.generateRandomRecords(schema, 1, 3L);
      shell.executeStatement(testTables.getUpdateQuery(tableName, newRecords.get(0)));
      HiveIcebergTestUtils.validateData(table, newRecords, 0);
    }
  }

  @Test
  public void testMultiInsert() {
    Assume.assumeTrue(fileFormat == FileFormat.PARQUET && isVectorized &&
        testTableType == TestTableType.HIVE_CATALOG);

    testTables.createTable(shell, "source", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    testTables.createTable(shell, "alice", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, null, formatVersion);
    testTables.createTable(shell, "green", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), fileFormat, null, formatVersion);

    String sql = "FROM source " +
        "INSERT INTO alice " +
        "  SELECT * WHERE first_name='Alice'" +
        "INSERT INTO green " +
        "  SELECT * WHERE last_name='Green'";
    shell.executeStatement(sql);

    List<Object[]> res = shell.executeStatement("SELECT * FROM alice");
    Assert.assertEquals(1, res.size());
    res = shell.executeStatement("SELECT * FROM green");
    Assert.assertEquals(1, res.size());
  }

  private static <T> PositionDelete<T> positionDelete(CharSequence path, long pos, T row) {
    PositionDelete<T> positionDelete = PositionDelete.create();
    return positionDelete.set(path, pos, row);
  }
}
