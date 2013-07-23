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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;

/**
 * Wraps an existing <code>HDataType</code> implementation as a terminated
 * version of itself. This has the useful side-effect of turning an existing
 * <code>HDataType</code> which is not <code>skippable</code> into a
 * <code>skippable</code> variant.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TerminatedWrapper<T> implements DataType<T> {

  protected final DataType<T> wrapped;
  protected final byte[] term;

  /**
   * Create a terminated version of the <code>wrapped</code>.
   * @throws IllegalArgumentException when <code>term</code> is null or empty.
   */
  public TerminatedWrapper(DataType<T> wrapped, byte[] term) {
    if (null == term || term.length == 0)
      throw new IllegalArgumentException("terminator must be non-null and non-empty.");
    this.wrapped = wrapped;
    wrapped.getOrder().apply(term);
    this.term = term;
  }

  /**
   * Create a terminated version of the <code>wrapped</code>.
   * <code>term</code> is converted to a <code>byte[]</code> using
   * {@link Bytes#toBytes(String)}.
   * @throws IllegalArgumentException when <code>term</code> is null or empty.
   */
  public TerminatedWrapper(DataType<T> wrapped, String term) {
    this(wrapped, Bytes.toBytes(term));
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
  public int encodedLength(T val) {
    return wrapped.encodedLength(val) + term.length;
  }

  @Override
  public Class<T> encodedClass() { return wrapped.encodedClass(); }

  /**
   * Return the position at which <code>term</code> begins within
   * <code>buff</code>, or <code>-1</code> if <code>term</code> is not found.
   */
  protected int terminatorPosition(ByteBuffer buff) {
    int i, limit;
    SKIP: for (i = buff.position(), limit = buff.limit(); i < limit; i++) {
      if (buff.array()[i] != term[0]) continue;
      int j;
      for (j = 1; j < term.length && i + j < buff.limit(); j++) {
        if (buff.array()[i + j] != term[j]) continue SKIP;
      }
      if (j == term.length) return i; // success
    }
    return -1;
  }

  /**
   * Skip the position of <code>buff</code> over the encoded value.
   * @throws IllegalArgumentException when the terminator sequence is not
   *           found. In the event this exception is thrown,
   *           <code>buff#position()</code> is restored to the original value.
   */
  @Override
  public void skip(ByteBuffer buff) {
    if (wrapped.isSkippable()) {
      wrapped.skip(buff);
      buff.position(buff.position() + term.length);
    } else {
      int skipTo = terminatorPosition(buff);
      if (-1 == skipTo) {
        throw new IllegalArgumentException("Terminator sequence not found.");
      }
      buff.position(skipTo + term.length);
    }
  }

  @Override
  public T decode(ByteBuffer buff) {
    T ret;
    if (wrapped.isSkippable()) {
      ret = wrapped.decode(buff);
    } else {
      ByteBuffer b = buff.duplicate();
      b.limit(terminatorPosition(buff));
      ret = wrapped.decode(b);
      buff.position(b.position());
    }
    buff.position(buff.position() + term.length);
    return ret;
  }

  /**
   * Write instance <code>val</code> into buffer <code>buff</code>.
   * @throws IllegalArgumentException when the encoded representation of
   *           <code>val</code> contains the <code>term</code> sequence. In
   *           the event this exception is thrown,
   *           <code>buff#position()</code> is restored the original value.
   */
  @Override
  public void encode(ByteBuffer buff, T val) {
    int start = buff.position();
    wrapped.encode(buff, val);
    ByteBuffer b = ByteBuffer.wrap(buff.array(), start, buff.position() - start);
    if (-1 != terminatorPosition(b)) {
      buff.position(start);
      throw new IllegalArgumentException("Encoded value contains terminator sequence.");
    }
    buff.put(term);
  }
}
