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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Order;

/**
 * <code>HDataType</code> is the base class for all HBase data types. Data
 * type implementations are designed to be serialized to and deserialized from
 * {@link ByteBuffer}s. Serialized representations can retain the natural sort
 * ordering of the source object, when a suitable encoding is supported by the
 * underlying implementation. This is a desirable feature for use in rowkeys
 * and column qualifiers.
 * <p>
 * Data type instances are designed to be stateless, thread-safe, and reused.
 * Implementations should provide <code>static final</code> instances
 * corresponding to each variation on configurable parameters. For instance,
 * order-preserving types should provide static ASCENDING and DESCENDING
 * instances. It is also encouraged for implementations operating on Java
 * primitive types to provide primitive implementations of the
 * <code>read</code> and <code>write</code> methods. This advice is a
 * performance consideration to clients reading and writing values in tight
 * loops.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface DataType<T> {

  /**
   * Indicates whether this instance writes encoded <code>byte[]</code>'s
   * which preserve the natural sort order of the unencoded value.
   * @return <code>true</code> when natural order is preserved,
   *         <code>false</code> otherwise.
   */
  public boolean isOrderPreserving();

  /**
   * Retrieve the sort {@link Order} honored by this data type, or
   * <code>null</code> when natural ordering is not preserved.
   */
  public Order getOrder();

  /**
   * Indicates whether this instance supports encoding <code>null</code>
   * values. This depends on the implementation details of the encoding
   * format. All <code>HDataType</code>s that support <code>null</code> should
   * treat <code>null</code> as comparing less than any <code>non-NULL</code>
   * value for default sort ordering purposes.
   * @return <code>true</code> when <code>null</code> is supported,
   *         <code>false</code> otherwise.
   */
  public boolean isNullable();

  /**
   * Indicates whether this instance is able to skip over it's encoded value
   * in a <code>ByteBuffer</code>. <code>HDataType</code>s that are not
   * skippable can only be used as the right-most field of a {@link Struct}.
   * @return
   */
  public boolean isSkippable();

  /**
   * Inform consumers how long the encoded <code>byte[]</code> will be.
   */
  public int encodedLength(T val);

  /**
   * Inform consumers over what type this <code>HDataType</code> operates.
   * @return
   */
  public Class<T> encodedClass();

  /**
   * Skip the position of <code>buff</code> over the encoded value.
   */
  public void skip(ByteBuffer buff);

  /**
   * Read an instance of <code>T</code> from the buffer <code>buff</code>.
   */
  public T decode(ByteBuffer buff);

  /**
   * Write instance <code>val</code> into buffer <code>buff</code>.
   */
  public void encode(ByteBuffer buff, T val);
}
