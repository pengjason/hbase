/**
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
package org.apache.hadoop.hbase.types;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Order;

/**
 * <p>
 * <code>Struct</code> is a simple {@link DataType} for implementing the
 * "compound rowkey" schema design strategy.
 * </p>
 * <h3>Encoding</h3>
 * <p>
 * <code>Struct</code> member values are encoded onto the target
 * <code>ByteBuffer</code> in the order in which they are declared. A
 * <code>Struct</code> may be used as a member of another <code>Struct</code>.
 * <code>Struct</code>s are not <code>nullable</code> but their component
 * fields may be.
 * </p>
 * <h3>Sort Order</h3>
 * <p>
 * <code>Struct</code> instances sort according to the composite order of
 * their fields, that is, left-to-right and depth-first. This can also be
 * thought of as lexicographic comparison of concatenated members.
 * </p>
 * <p>
 * {@link StructIterator} is provided as a convenience for consuming the
 * sequence of values. Users may find it more appropriate to provide their own
 * custom {@link DataType} for encoding application objects rather than using
 * this <code>Object[]</code> implementation. Examples are provided in test.
 * </p>
 * @see StructIterator
 * @see DataType#isNullable()
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Struct implements DataType<Object[]> {

  @SuppressWarnings("rawtypes")
  protected final DataType[] fields;
  protected final boolean isOrderPreserving;
  protected final boolean isSkippable;

  /**
   * Create a new <code>Struct</code> instance defined as the sequence of
   * <code>HDataType</code>s in <code>memberTypes</code>.
   * <p>
   * A <code>Struct<code> is <code>orderPreserving</code> when all of its
   * fields are <code>orderPreserving</code>. A <code>Struct</code> is
   * <code>skippable</code> when all of its fields are <code>skippable</code>.
   * </p>
   */
  @SuppressWarnings("rawtypes")
  public Struct(DataType[] memberTypes) {
    this.fields = memberTypes;
    // a Struct is not orderPreserving when any of its fields are not.
    boolean preservesOrder = true;
    // a Struct is not skippable when any of its fields are not.
    boolean skippable = true;
    for (int i = 0; i < this.fields.length; i++) {
      DataType dt = this.fields[i];
      if (!dt.isOrderPreserving()) preservesOrder = false;
      if (i < this.fields.length - 2 && !dt.isSkippable())
        throw new IllegalArgumentException("Non-right-most struct fields must be skippable.");
      if (!dt.isSkippable()) skippable = false;
    }
    this.isOrderPreserving = preservesOrder;
    this.isSkippable = skippable;
  }

  @Override
  public boolean isOrderPreserving() { return isOrderPreserving; }

  @Override
  public Order getOrder() { return null; }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public boolean isSkippable() { return isSkippable; }

  @SuppressWarnings("unchecked")
  @Override
  public int encodedLength(Object[] val) {
    assert fields.length == val.length;
    int sum = 0;
    for (int i = 0; i < fields.length; i++)
      sum += fields[i].encodedLength(val[i]);
    return sum;
  }

  @Override
  public Class<Object[]> encodedClass() { return Object[].class; }

  /**
   * Retrieve an {@link Iterator} over the values encoded in <code>b</code>.
   * The <code>byte[]</code> backing <code>b</code> is not modified by use of
   * the <code>Iterator</code>, however the state of <code>b</code> is. To
   * create multiple <code>Iterator</code>s over the same backing
   * <code>ByteBuffer</code>, construct the <code>Iterator</code>s over
   * duplicates ( {@link ByteBuffer#duplicate()} ) of <code>b</code>.
   */
  public StructIterator iterator(ByteBuffer buff) {
    return new StructIterator(buff, fields);
  }

  @Override
  public void skip(ByteBuffer buff) {
    StructIterator it = iterator(buff);
    while (it.hasNext())
      it.skip();
  }

  @Override
  public Object[] decode(ByteBuffer buff) {
    ArrayList<Object> values = new ArrayList<Object>(fields.length);
    Iterator<Object> it = iterator(buff);
    while (it.hasNext())
      values.add(it.next());
    assert values.size() == fields.length;
    return values.toArray();
  }

  /**
   * Read the field at <code>position</code>. <code>buff</code> is left
   * unmodified.
   */
  public Object read(ByteBuffer buff, int position) {
    assert position >= 0;
    ByteBuffer b = buff.duplicate();
    StructIterator it = iterator(b);
    for (; position > 0; position--)
      it.skip();
    return it.next();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void encode(ByteBuffer buff, Object[] val) {
    assert fields.length == val.length;
    for (int i = 0; i < fields.length; i++) {
      fields[i].encode(buff, val[i]);
    }
  }
}
