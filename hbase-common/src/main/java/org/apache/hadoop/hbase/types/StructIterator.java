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
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An {@link Iterator} over encoded <code>Struct</code> members.
 * <p>
 * This iterates over each serialized <code>Struct</code> field from the
 * specified <code>HDataTypes<?>[]</code> definition. It allows you to read
 * the field or skip over its serialized bytes using {@link #next()} and
 * {@link #skip()}, respectively. This is in contrast to the
 * <code>Struct</code> method which allow you to
 * {@link Struct#decode(ByteBuffer)} or {@link Struct#skip(ByteBuffer)} over the
 * entire <code>Struct</code> at once.
 * </p>
 * <p>
 * This iterator may also be used to read bytes from any <code>Struct</code>
 * for which the specified <code>HDataType<?>[]</code> is a prefix. For
 * example, if the specified <code>Struct</code> definition has a
 * {@link OrderedNumeric} and a {@link OrderedString} field, you may parse the serialized
 * output of a <code>Struct</code> whose fields are {@link OrderedNumeric},
 * {@link OrderedString}, and {@link Binary}. The iterator would return a number
 * followed by a <code>String</code>. The trailing <code>byte[]</code> would
 * be ignored.
 * </p>
 */
public class StructIterator implements Iterator<Object> {

  protected final ByteBuffer buff;
  @SuppressWarnings("rawtypes")
  protected final DataType[] types;
  protected int idx = 0;

  /**
   * Construct <code>StructIterator</code> over the values encoded in
   * <code>buff</code> using the specified <code>types</code> definition.
   * @param buff The buffer from which to read encoded values.
   * @param types The sequence of types to use as the schema for this
   *          <code>Struct</code>.
   */
  public StructIterator(final ByteBuffer buff, @SuppressWarnings("rawtypes") DataType[] types) {
    this.buff = buff;
    this.types = types;
  }

  @Override
  public boolean hasNext() {
    return idx < types.length && buff.position() != buff.limit();
  }

  @Override
  public void remove() { throw new UnsupportedOperationException(); }

  @Override
  public Object next() {
    if (!hasNext()) throw new NoSuchElementException();
    return types[idx++].decode(buff);
  }

  /**
   * Bypass the next encoded value.
   */
  public void skip() {
    if (!hasNext()) throw new NoSuchElementException();
    types[idx++].skip(buff);
  }
}
