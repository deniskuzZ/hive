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

package org.apache.hadoop.hive.ql.exec.vector.reducesink;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.PartitionedOutputCollector;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.shuffle.ColumnarShuffleBlock;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkInfo;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Assert;
import org.junit.Test;

/**
 * The hashCode the ObjectHash ReduceSink gives a row must not depend on whether its partition columns
 * arrive repeating: each test runs the same rows once with repeating partition columns and once
 * flattened, and compares the hashCode of every row the operator emits. Its columnar shuffle must send
 * every row to the partition of that hashCode.
 */
public class TestVectorReduceSinkObjectHashOperator {

  private static final int SIZE = 100;

  private static final String[] NAMES = {"path", "bucket", "id", "value"};
  private static final TypeInfo[] TYPES = {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.intTypeInfo,
      TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo};
  private static final int PATH = 0;
  private static final int BUCKET = 1;
  private static final int ID = 2;
  private static final int VALUE = 3;
  private static final int PARTITIONS = 7;

  @Test
  public void testRepeatingPartitionColumn() throws Exception {
    assertSameHashCodes(new int[] {PATH}, false, () -> List.of(batch("a", 0), batch("b", SIZE)));
  }

  @Test
  public void testRepeatingNullPartitionColumn() throws Exception {
    assertSameHashCodes(new int[] {PATH}, false, () -> List.of(batch(null, 0), batch("a", SIZE)));
  }

  @Test
  public void testRepeatingPartitionColumnWithSelectedRows() throws Exception {
    assertSameHashCodes(new int[] {PATH}, false, () -> {
      VectorizedRowBatch batch = batch("a", 0);
      batch.selectedInUse = true;
      int size = 0;
      for (int i = 0; i < SIZE; i += 3) {
        batch.selected[size++] = i;
      }
      batch.size = size;
      return List.of(batch);
    });
  }

  @Test
  public void testRepeatingPartitionColumnWithBucketColumn() throws Exception {
    assertSameHashCodes(new int[] {PATH}, true, () -> List.of(batch("a", 0), batch("b", SIZE)));
  }

  @Test
  public void testRepeatingAndNonRepeatingPartitionColumns() throws Exception {
    assertSameHashCodes(new int[] {PATH, ID}, false, () -> List.of(batch("a", 0)));
  }

  /** A repeating path goes whole to one partition; a path with an id goes per row. */
  @Test
  public void testColumnarPartitionsAreTheRowPathPartitions() throws Exception {
    assertColumnarPartitions(new int[] {PATH}, () -> List.of(batch("a", 0), batch(null, SIZE), batch("b", 2 * SIZE)));
    assertColumnarPartitions(new int[] {PATH, ID}, () -> List.of(batch("a", 0), batch("b", SIZE)));
    assertColumnarPartitions(new int[] {PATH}, () -> {
      VectorizedRowBatch batch = batch("a", 0);
      batch.selectedInUse = true;
      int size = 0;
      for (int i = 0; i < SIZE; i += 3) {
        batch.selected[size++] = i;
      }
      batch.size = size;
      return List.of(batch);
    });
  }

  private static void assertColumnarPartitions(int[] partitionColumns, Batches batches) throws HiveException {
    List<Integer> hashCodes = hashCodes(partitionColumns, false, batches.get());
    Map<Long, Integer> expected = new HashMap<>();
    int row = 0;
    for (VectorizedRowBatch batch : batches.get()) {
      for (int k = 0; k < batch.size; k++) {
        long value = ((LongColumnVector) batch.cols[VALUE]).vector[batch.selectedInUse ? batch.selected[k] : k];
        expected.put(value, (hashCodes.get(row++) & Integer.MAX_VALUE) % PARTITIONS);
      }
    }
    BlockCollector blocks = new BlockCollector();
    run(newOperator(true, partitionColumns, false), blocks, batches.get());
    Assert.assertEquals(expected, blocks.partitionOfValue);
  }

  /** Decodes the blocks of a columnar shuffle: the partition of every row, by its value. */
  private static final class BlockCollector implements OutputCollector<Object, Object>, PartitionedOutputCollector {
    final Map<Long, Integer> partitionOfValue = new HashMap<>();

    @Override
    public int getNumPartitions() {
      return PARTITIONS;
    }

    @Override
    public void collect(Object key, Object value) {
      BytesWritable block = (BytesWritable) value;
      LongColumnVector values = new LongColumnVector(SIZE);
      int rows = ColumnarShuffleBlock.decode(ByteBuffer.wrap(block.getBytes(), 0, block.getLength()),
          new ColumnVector[] {values});
      for (int i = 0; i < rows; i++) {
        Assert.assertNull(partitionOfValue.put(values.vector[i], key.hashCode()));
      }
    }
  }

  private interface Batches {
    List<VectorizedRowBatch> get();
  }

  private static void assertSameHashCodes(int[] partitionColumns, boolean bucketed, Batches batches)
      throws HiveException {
    List<VectorizedRowBatch> flat = batches.get();
    for (VectorizedRowBatch batch : flat) {
      for (ColumnVector column : batch.cols) {
        column.flatten(batch.selectedInUse, batch.selected, batch.size);
      }
    }
    List<Integer> expected = hashCodes(partitionColumns, bucketed, flat);
    Assert.assertEquals(flat.stream().mapToInt(b -> b.size).sum(), expected.size());
    Assert.assertEquals(expected, hashCodes(partitionColumns, bucketed, batches.get()));
  }

  /** path repeats (null when path is null); bucket, id and value differ per row. */
  private static VectorizedRowBatch batch(String path, int firstId) {
    VectorizedRowBatch batch = new VectorizedRowBatch(NAMES.length, SIZE);
    BytesColumnVector pathColumn = new BytesColumnVector(SIZE);
    pathColumn.initBuffer();
    if (path == null) {
      pathColumn.isRepeating = true;
      pathColumn.noNulls = false;
      pathColumn.isNull[0] = true;
    } else {
      pathColumn.fill(path.getBytes(StandardCharsets.UTF_8));
    }
    LongColumnVector bucketColumn = new LongColumnVector(SIZE);
    LongColumnVector idColumn = new LongColumnVector(SIZE);
    LongColumnVector valueColumn = new LongColumnVector(SIZE);
    for (int i = 0; i < SIZE; i++) {
      bucketColumn.vector[i] = (firstId + i) % 7;
      idColumn.vector[i] = firstId + i;
      valueColumn.vector[i] = 10L * (firstId + i);
    }
    batch.cols[PATH] = pathColumn;
    batch.cols[BUCKET] = bucketColumn;
    batch.cols[ID] = idColumn;
    batch.cols[VALUE] = valueColumn;
    batch.size = SIZE;
    return batch;
  }

  /** Runs an empty-key ObjectHash ReduceSink over the batches and returns the hashCode of every row. */
  private static List<Integer> hashCodes(int[] partitionColumns, boolean bucketed, List<VectorizedRowBatch> batches)
      throws HiveException {
    List<Integer> hashCodes = new ArrayList<>();
    run(newOperator(false, partitionColumns, bucketed), (key, value) -> hashCodes.add(((HiveKey) key).hashCode()),
        batches);
    return hashCodes;
  }

  private static void run(VectorReduceSinkObjectHashOperator operator, OutputCollector<Object, Object> out,
      List<VectorizedRowBatch> batches) throws HiveException {
    HiveConf conf = new HiveConf();
    // The operator names its task from the map work when debug logging is on.
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "file:/tmp");
    Utilities.setMapWork(conf, new MapWork("Map 1"));
    operator.setOutputCollector(out);
    try {
      operator.initialize(conf, new ObjectInspector[] {null});
      for (VectorizedRowBatch batch : batches) {
        operator.process(batch, 0);
      }
      operator.close(false);
    } finally {
      Utilities.clearWorkMapForConf(conf);
    }
  }

  /** An empty-key ObjectHash ReduceSink, of the row path or its columnar shuffle. */
  private static VectorReduceSinkObjectHashOperator newOperator(boolean columnar, int[] partitionColumns,
      boolean bucketed) throws HiveException {
    HiveConf conf = new HiveConf();
    List<DataTypePhysicalVariation> variations =
        Collections.nCopies(NAMES.length, DataTypePhysicalVariation.NONE);
    VectorizationContext vContext =
        new VectorizationContext("test", Arrays.asList(NAMES), Arrays.asList(TYPES), variations, conf);

    List<ExprNodeDesc> partitionCols = new ArrayList<>();
    for (int column : partitionColumns) {
      partitionCols.add(column(column));
    }
    ReduceSinkDesc desc = PlanUtils.getReduceSinkDesc(new ArrayList<>(), List.of(column(VALUE)), List.of("_col0"),
        false, -1, partitionCols, "", "", NullOrdering.NULLS_LAST, -1, AcidUtils.Operation.NOT_ACID, false);
    desc.setNumReducers(16);
    desc.setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.AUTOPARALLEL));
    desc.setBucketingVersion(2);

    VectorReduceSinkInfo info = new VectorReduceSinkInfo();
    info.setUseUniformHash(false);
    info.setReduceSinkValueColumnMap(new int[] {VALUE});
    info.setReduceSinkValueTypeInfos(new TypeInfo[] {TYPES[VALUE]});
    info.setReduceSinkValueColumnVectorTypes(new Type[] {Type.LONG});
    info.setReduceSinkPartitionColumnMap(partitionColumns);
    info.setReduceSinkPartitionTypeInfos(Arrays.stream(partitionColumns).mapToObj(c -> TYPES[c])
        .toArray(TypeInfo[]::new));
    info.setReduceSinkPartitionColumnVectorTypes(Arrays.stream(partitionColumns)
        .mapToObj(c -> c == PATH ? Type.BYTES : Type.LONG).toArray(Type[]::new));
    if (bucketed) {
      desc.setBucketCols(List.of(column(BUCKET)));
      desc.setNumBuckets(8);
      info.setReduceSinkBucketColumnMap(new int[] {BUCKET});
      info.setReduceSinkBucketTypeInfos(new TypeInfo[] {TYPES[BUCKET]});
      info.setReduceSinkBucketColumnVectorTypes(new Type[] {Type.LONG});
    }

    VectorReduceSinkDesc vectorDesc = new VectorReduceSinkDesc();
    vectorDesc.setVectorReduceSinkInfo(info);
    vectorDesc.setIsEmptyKey(true);
    vectorDesc.setIsEmptyValue(false);
    vectorDesc.setIsEmptyBuckets(!bucketed);
    vectorDesc.setIsEmptyPartitions(false);
    vectorDesc.setIsKeyBinarySortable(true);
    vectorDesc.setIsValueLazyBinary(true);

    return columnar
        ? new VectorReduceSinkColumnarObjectHashOperator(new CompilationOpContext(), desc, vContext, vectorDesc)
        : new VectorReduceSinkObjectHashOperator(new CompilationOpContext(), desc, vContext, vectorDesc);
  }

  private static ExprNodeDesc column(int column) {
    return new ExprNodeColumnDesc(TYPES[column], NAMES[column], "t", false);
  }
}
