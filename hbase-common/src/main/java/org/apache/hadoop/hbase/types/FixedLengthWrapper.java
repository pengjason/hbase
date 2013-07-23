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
 * Wraps an existing <code>HDataType</code> implementation as a fixed-length
 * version of itself. This has the useful side-effect of turning an existing
 * <code>HDataType</code> which is not <code>skippable</code> into a
 * <code>skippable</code> variant.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FixedLengthWrapper<T> implements DataType<T> {

  protected final DataType<T> wrapped;
  protected final int length;

  /**
   * Create a fixed-length version of the <code>wrapped</code>.
   */
  public FixedLengthWrapper(DataType<T> wrapped, int length) {
    this.wrapped = wrapped;
    this.length = length;
  }

  @Override
  public boolean isOrderPreserving() { return wrapped.isOrderPreserving(); }

  @Override
  public Order getOrder() { return wrapped.getOrder(); }

  @Override
  public boolean isNullable() { return wrapped.isNullable(); }

  @Override
  public boolean isSkippable() { return true; }

  @Override
  public int encodedLength(T val) { return length; }

  @Override
  public Class<T> encodedClass() { return wrapped.encodedClass(); }

  @Override
  public void skip(ByteBuffer buff) { buff.position(buff.position() + length); }

  /**
   * Read an instance of <code>T</code> from the buffer <code>buff</code>.
   * @throws IllegalArgumentException when <code>buff</code> lacks sufficient
   *           remaining capacity. In the event this exception is thrown,
   *           <code>buff#position()</code> is restored to the original value.
   */
  @Override
  public T decode(ByteBuffer buff) {
    if (buff.remaining() < length)
      throw new IllegalArgumentException("Not enough buffer remaining.");
    ByteBuffer b = buff.duplicate();
    b.limit(b.position() + length);
    T ret = wrapped.decode(b);
    buff.position(buff.position() + length);
    return ret;
  }

  /**
   * Write instance <code>val</code> into buffer <code>buff</code>. Any space
   * remaining after <code>val</code> is written is zero-padded up to
   * <code>length</code>.
   * @throws IllegalArgumentException when <code>buff</code> lacks sufficient
   *           remaining capacity. In the event this exception is thrown,
   *           <code>buff#position()</code> is restored the original value.
   */
  @Override
  public void encode(ByteBuffer buff, T val) {
    if (buff.remaining() < length)
      throw new IllegalArgumentException("Not enough buffer remaining.");
    ByteBuffer b = buff.duplicate();
    b.limit(b.position() + length);
    wrapped.encode(b, val);
    while (b.position() != b.limit()) b.put((byte) 0x00);
    buff.position(buff.position() + length);
  }
}
