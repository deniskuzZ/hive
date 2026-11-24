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
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.benchmark.vectorization.ColumnVectorGenUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestMurmurHashExpression {

  private Random rand = new Random(TestMurmurHashExpression.class.getName().getBytes().hashCode());
  private int SIZE = VectorizedRowBatch.DEFAULT_SIZE;

  @Test
  public void testMurmurHashStringColIntCol() throws HiveException {
    BytesColumnVector cvString = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, false, SIZE, rand);
    LongColumnVector cvInt = (LongColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("int"), false, false, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString;
    vrb.cols[1] = cvInt;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashStringColIntCol(0, 1, 2).evaluate(vrb);

    for (int i = 0; i < SIZE; i++) {
      Text t = new Text();
      t.set(cvString.vector[i], cvString.start[i], cvString.length[i]);
      Assert.assertEquals(
          ObjectInspectorUtils.getBucketHashCode(
              new Object[] { t, new LongWritable(cvInt.vector[i]) },
              new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                  PrimitiveObjectInspectorFactory.writableLongObjectInspector }),
          ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  @Test
  public void testMurmurHashStringColStringCol() throws HiveException {
    BytesColumnVector cvString1 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, false, SIZE, rand);
    BytesColumnVector cvString2 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, false, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString1;
    vrb.cols[1] = cvString2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashStringColStringCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(false, vrb.cols[2].isRepeating); // non-repeating

    for (int i = 0; i < SIZE; i++) {
      Text t1 = new Text();
      t1.set(cvString1.vector[i], cvString1.start[i], cvString1.length[i]);
      Text t2 = new Text();
      t2.set(cvString2.vector[i], cvString2.start[i], cvString2.length[i]);

      Assert.assertEquals(
          ObjectInspectorUtils.getBucketHashCode(new Object[] { t1, t2 },
              new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                  PrimitiveObjectInspectorFactory.writableStringObjectInspector }),
          ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  @Test
  public void testMurmurHashIntColIntCol() throws HiveException {
    LongColumnVector cvInt1 = (LongColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("int"), false, false, SIZE, rand);
    LongColumnVector cvInt2 = (LongColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("int"), false, false, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvInt1;
    vrb.cols[1] = cvInt2;

    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashIntColIntCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(false, vrb.cols[2].isRepeating); // non-repeating

    for (int i = 0; i < SIZE; i++) {
      Assert.assertEquals(ObjectInspectorUtils.getBucketHashCode(
          new Object[] { new LongWritable(cvInt1.vector[i]), new LongWritable(cvInt2.vector[i]) },
          new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableLongObjectInspector,
              PrimitiveObjectInspectorFactory.writableLongObjectInspector }),
          ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  @Test
  public void testMurmurHashRepeating() throws HiveException {
    BytesColumnVector cvString1 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, true, SIZE, rand);
    BytesColumnVector cvString2 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, true, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString1;
    vrb.cols[1] = cvString2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashStringColStringCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(true, vrb.cols[2].isRepeating); // both of the inputs were repeating

    Text t1 = new Text();
    t1.set(cvString1.vector[0], cvString1.start[0], cvString1.length[0]);
    Text t2 = new Text();
    t2.set(cvString2.vector[0], cvString2.start[0], cvString2.length[0]);

    // output's first element is the hash of first input elements
    Assert.assertEquals(
        ObjectInspectorUtils.getBucketHashCode(new Object[] { t1, t2 },
            new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector }),
        ((LongColumnVector) vrb.cols[2]).vector[0]);
  }

  @Test
  public void testMurmurHashRepeatingBothNulls() throws HiveException {
    BytesColumnVector cvString1 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, true, SIZE, rand);
    BytesColumnVector cvString2 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, true, SIZE, rand);

    cvString1.noNulls = false;
    cvString1.isNull[0] = true;
    cvString2.noNulls = false;
    cvString2.isNull[0] = true;

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString1;
    vrb.cols[1] = cvString2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    // fake output value to test short-circuiting
    ((LongColumnVector) vrb.cols[2]).vector[1] = 1234;

    new MurmurHashStringColStringCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(true, vrb.cols[2].isRepeating); // both of the inputs were repeating

    // output's first element is 0, which is hash of null elements
    Assert.assertEquals(0, ((LongColumnVector) vrb.cols[2]).vector[0]);

    // if isRepeating, vectorization logic is not supposed to touch other elements than 0th
    Assert.assertEquals(1234, ((LongColumnVector) vrb.cols[2]).vector[1]);
  }

  @Test
  public void testMurmurHashWithNullsString() throws HiveException {
    BytesColumnVector cvString1 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), true, false, SIZE, rand);
    BytesColumnVector cvString2 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), true, false, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString1;
    vrb.cols[1] = cvString2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashStringColStringCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(false, vrb.cols[2].isRepeating); // non-repeating

    for (int i = 0; i < SIZE; i++) {
      Text t1 = null;
      if (!cvString1.isNull[i]) {
        t1 = new Text();
        t1.set(cvString1.vector[i], cvString1.start[i], cvString1.length[i]);
      }

      Text t2 = null;
      if (!cvString2.isNull[i]) {
        t2 = new Text();
        t2.set(cvString2.vector[i], cvString2.start[i], cvString2.length[i]);
      }

      Assert.assertEquals(
          ObjectInspectorUtils.getBucketHashCode(new Object[] { t1, t2 },
              new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                  PrimitiveObjectInspectorFactory.writableStringObjectInspector }),
          ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  @Test
  public void testMurmurHashWithNullsInt() throws HiveException {
    LongColumnVector cvInt1 = (LongColumnVector) ColumnVectorGenUtil
        .generateColumnVector(TypeInfoFactory.getPrimitiveTypeInfo("int"), true, false, SIZE, rand);
    LongColumnVector cvInt2 = (LongColumnVector) ColumnVectorGenUtil
        .generateColumnVector(TypeInfoFactory.getPrimitiveTypeInfo("int"), true, false, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvInt1;
    vrb.cols[1] = cvInt2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashIntColIntCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(false, vrb.cols[2].isRepeating); // non-repeating

    for (int i = 0; i < SIZE; i++) {
      Assert.assertEquals(
          ObjectInspectorUtils.getBucketHashCode(
              new Object[] { cvInt1.isNull[i] ? null : new LongWritable(cvInt1.vector[i]),
                  cvInt2.isNull[i] ? null : new LongWritable(cvInt2.vector[i]) },
              new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableLongObjectInspector,
                  PrimitiveObjectInspectorFactory.writableLongObjectInspector }),
          ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }


  private BytesColumnVector createBytesColumnVector(Text value, boolean isRepeating) {
    BytesColumnVector bcv = new BytesColumnVector(SIZE);
    bcv.initBuffer(value.getLength());
    bcv.noNulls = true;
    bcv.isRepeating = isRepeating;

    byte[] bytes = value.copyBytes();
    if (isRepeating) {
      bcv.setRef(0, bytes, 0, value.getLength());
      for (int i = 1; i < SIZE; i++) {
        // modify length[i] to mimic ColumnVector reusing.
        bcv.length[i] = 10;
      }
    } else {
      for (int i = 0; i < SIZE; i++) {
        bcv.setRef(i, bytes, 0, value.getLength());
      }
    }

    return bcv;
  }

  private LongColumnVector createLongColumnVector(long value, boolean isRepeating) {
    LongColumnVector lcv = new LongColumnVector(SIZE);
    lcv.noNulls = true;
    lcv.isRepeating = isRepeating;
    if (isRepeating) {
      lcv.vector[0] = value;
    } else {
      for (int i = 0; i < SIZE; i++) {
        lcv.vector[i] = value;
      }
    }

    return lcv;
  }

  @Test
  public void testMurmurHashStringColStringColSingleRepeating() throws HiveException {
    Text text = new Text("Value");
    BytesColumnVector cvString1 = createBytesColumnVector(text, false);
    BytesColumnVector cvString2 = createBytesColumnVector(text, true);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString1;
    vrb.cols[1] = cvString2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashStringColStringCol(0, 1, 2).evaluate(vrb);

    Assert.assertFalse(vrb.cols[2].isRepeating);

    for (int i = 0; i < SIZE; i++) {
      Assert.assertEquals(-452676736, ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  @Test
  public void testMurmurHashIntColIntColSingleRepeating() throws HiveException {
    long num = 7640254796529531955L;
    LongColumnVector cvLong1 = createLongColumnVector(num, false);
    LongColumnVector cvLong2 = createLongColumnVector(num, true);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvLong1;
    vrb.cols[1] = cvLong2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashIntColIntCol(0, 1, 2).evaluate(vrb);

    Assert.assertFalse(vrb.cols[2].isRepeating);

    for (int i = 0; i < SIZE; i++) {
      Assert.assertEquals(1533409056, ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  @Test
  public void testMurmurHashNonRepeatingStringColRepeatingIntCol() throws HiveException {
    Text text = new Text("Value");
    long num = 7640254796529531955L;

    BytesColumnVector cvString = createBytesColumnVector(text, false);
    LongColumnVector cvLong = createLongColumnVector(num, true);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString;
    vrb.cols[1] = cvLong;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashStringColIntCol(0, 1, 2).evaluate(vrb);

    Assert.assertFalse(vrb.cols[2].isRepeating);

    for (int i = 0; i < SIZE; i++) {
      Assert.assertEquals(1622654365, ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  @Test
  public void testMurmurHashRepeatingStringColNonRepeatingIntCol() throws HiveException {
    Text text = new Text("Value");
    long num = 7640254796529531955L;

    BytesColumnVector cvString = createBytesColumnVector(text, true);
    LongColumnVector cvLong = createLongColumnVector(num, false);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString;
    vrb.cols[1] = cvLong;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashStringColIntCol(0, 1, 2).evaluate(vrb);

    Assert.assertFalse(vrb.cols[2].isRepeating);

    for (int i = 0; i < SIZE; i++) {
      Assert.assertEquals(1622654365, ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  /**
   * The output is a scratch column that an earlier expression of the batch left with NULLs. A hash is never NULL,
   * so every row it writes must read as not NULL.
   */
  @Test
  public void testMurmurHashOverwritesNullsOfItsOutput() throws HiveException {
    for (boolean selectedInUse : new boolean[] { false, true }) {
      LongColumnVector output = new LongColumnVector(SIZE);
      output.noNulls = false;
      Arrays.fill(output.isNull, true);

      VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
      vrb.cols[0] = ColumnVectorGenUtil.generateColumnVector(
          TypeInfoFactory.getPrimitiveTypeInfo("int"), false, false, SIZE, rand);
      vrb.cols[1] = ColumnVectorGenUtil.generateColumnVector(
          TypeInfoFactory.getPrimitiveTypeInfo("int"), false, false, SIZE, rand);
      vrb.cols[2] = output;
      if (selectedInUse) {
        vrb.selectedInUse = true;
        vrb.size = SIZE / 2;
        for (int j = 0; j < vrb.size; j++) {
          vrb.selected[j] = 2 * j + 1;
        }
      }

      new MurmurHashIntColIntCol(0, 1, 2).evaluate(vrb);

      for (int j = 0; j < vrb.size; j++) {
        int i = selectedInUse ? vrb.selected[j] : j;
        Assert.assertTrue("row " + i, output.noNulls || !output.isNull[i]);
      }
    }
  }

  /**
   * The key of a multi-column semijoin, hash(hash(a, b), c), with both scratch columns left with NULLs by earlier
   * expressions of the batch: the result equals the one over clean scratch columns.
   */
  @Test
  public void testNestedMurmurHashOverScratchColumnsWithNulls() throws HiveException {
    VectorizedRowBatch clean = new VectorizedRowBatch(5, SIZE);
    for (int c = 0; c < 3; c++) {
      clean.cols[c] = ColumnVectorGenUtil.generateColumnVector(
          TypeInfoFactory.getPrimitiveTypeInfo("bigint"), false, false, SIZE, rand);
    }
    clean.cols[3] = new LongColumnVector(SIZE);
    clean.cols[4] = new LongColumnVector(SIZE);
    VectorizedRowBatch dirty = new VectorizedRowBatch(5, SIZE);
    System.arraycopy(clean.cols, 0, dirty.cols, 0, 3);
    for (int c = 3; c < 5; c++) {
      dirty.cols[c] = new LongColumnVector(SIZE);
      dirty.cols[c].noNulls = false;
      Arrays.fill(dirty.cols[c].isNull, true);
    }

    for (VectorizedRowBatch vrb : new VectorizedRowBatch[] { clean, dirty }) {
      MurmurHashIntColIntCol outer = new MurmurHashIntColIntCol(3, 2, 4);
      outer.setChildExpressions(new VectorExpression[] { new MurmurHashIntColIntCol(0, 1, 3) });
      outer.evaluate(vrb);
    }

    LongColumnVector expected = (LongColumnVector) clean.cols[4];
    LongColumnVector actual = (LongColumnVector) dirty.cols[4];
    for (int i = 0; i < SIZE; i++) {
      Assert.assertTrue("row " + i, actual.noNulls || !actual.isNull[i]);
      Assert.assertEquals("row " + i, expected.vector[i], actual.vector[i]);
    }
  }

  /**
   * Repeating inputs without NULLs, stale isNull entries in them and NULLs left in the output.
   */
  @Test
  public void testMurmurHashRepeatingOverStaleNulls() throws HiveException {
    long num1 = 42L;
    long num2 = 7640254796529531955L;
    LongColumnVector cvLong1 = createLongColumnVector(num1, true);
    LongColumnVector cvLong2 = createLongColumnVector(num2, true);
    cvLong1.isNull[0] = true;
    cvLong2.isNull[0] = true;
    LongColumnVector output = new LongColumnVector(SIZE);
    output.noNulls = false;
    Arrays.fill(output.isNull, true);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvLong1;
    vrb.cols[1] = cvLong2;
    vrb.cols[2] = output;

    new MurmurHashIntColIntCol(0, 1, 2).evaluate(vrb);

    Assert.assertTrue(output.isRepeating);
    Assert.assertFalse(output.isNull[0]);
    ObjectInspector longOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    Assert.assertEquals(ObjectInspectorUtils.getBucketHashCode(
        new Object[] { new LongWritable(num1), new LongWritable(num2) }, new ObjectInspector[] { longOI, longOI }),
        output.vector[0]);
  }

  /**
   * isNull holds no information while noNulls is set, so the values of a column without NULLs are hashed whatever
   * its isNull says.
   */
  @Test
  public void testMurmurHashReadsIsNullOnlyWithNulls() throws HiveException {
    LongColumnVector cvInt1 = (LongColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("int"), false, false, SIZE, rand);
    LongColumnVector cvInt2 = (LongColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("int"), false, false, SIZE, rand);
    BytesColumnVector cvString1 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, false, SIZE, rand);
    BytesColumnVector cvString2 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, false, SIZE, rand);
    for (ColumnVector cv : new ColumnVector[] { cvInt1, cvInt2, cvString1, cvString2 }) {
      cv.noNulls = true;
      Arrays.fill(cv.isNull, true);
    }

    VectorizedRowBatch vrb = new VectorizedRowBatch(7, SIZE);
    vrb.cols[0] = cvInt1;
    vrb.cols[1] = cvInt2;
    vrb.cols[2] = cvString1;
    vrb.cols[3] = cvString2;
    vrb.cols[4] = new LongColumnVector(SIZE);
    vrb.cols[5] = new LongColumnVector(SIZE);
    vrb.cols[6] = new LongColumnVector(SIZE);

    new MurmurHashIntColIntCol(0, 1, 4).evaluate(vrb);
    new MurmurHashStringColIntCol(2, 0, 5).evaluate(vrb);
    new MurmurHashStringColStringCol(2, 3, 6).evaluate(vrb);

    ObjectInspector longOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    for (int i = 0; i < SIZE; i++) {
      LongWritable long1 = new LongWritable(cvInt1.vector[i]);
      Text text1 = new Text();
      text1.set(cvString1.vector[i], cvString1.start[i], cvString1.length[i]);
      Text text2 = new Text();
      text2.set(cvString2.vector[i], cvString2.start[i], cvString2.length[i]);
      Assert.assertEquals(ObjectInspectorUtils.getBucketHashCode(
          new Object[] { long1, new LongWritable(cvInt2.vector[i]) }, new ObjectInspector[] { longOI, longOI }),
          ((LongColumnVector) vrb.cols[4]).vector[i]);
      Assert.assertEquals(ObjectInspectorUtils.getBucketHashCode(
          new Object[] { text1, long1 }, new ObjectInspector[] { stringOI, longOI }),
          ((LongColumnVector) vrb.cols[5]).vector[i]);
      Assert.assertEquals(ObjectInspectorUtils.getBucketHashCode(
          new Object[] { text1, text2 }, new ObjectInspector[] { stringOI, stringOI }),
          ((LongColumnVector) vrb.cols[6]).vector[i]);
    }
  }
}
