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
 * An <code>HDataType</code> for interacting with variable-length values
 * encoded using {@link Bytes#putBytes(byte[], int, byte[], int, int)}.
 * Intended to make it easier to transition away from direct use of
 * {@link Bytes}.
 * @see Bytes#putBytes(byte[], int, byte[], int, int)
 * @see LegacyBytesTerminated
 * @see OrderedBlob
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LegacyBytes implements DataType<byte[]> {

  protected final Order order;

  public LegacyBytes() { order = Order.ASCENDING; }
  public LegacyBytes(Order order) { this.order = order; }

  @Override
  public boolean isOrderPreserving() { return true; }

  @Override
  public Order getOrder() { return order; }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public boolean isSkippable() { return false; }

  @Override
  public void skip(ByteBuffer buff) { buff.position(buff.limit()); }

  @Override
  public int encodedLength(byte[] val) { return val.length; }

  @Override
  public Class<byte[]> encodedClass() { return byte[].class; }

  /**
   * Skip <code>buff</code> ahead <code>length</code> bytes.
   */
  public void skip(ByteBuffer buff, int length) {
    buff.position(buff.position() + length);
  }

  @Override
  public byte[] decode(ByteBuffer buff) {
    return decode(buff, buff.limit() - buff.position());
  }

  @Override
  public void encode(ByteBuffer buff, byte[] val) {
    buff.put(val);
  }

  /**
   * Read a <code>byte[]</code> from the buffer <code>buff</code>.
   */
  public byte[] decode(ByteBuffer buff, int length) {
    byte[] val = new byte[length];
    buff.get(val);
    return val;
  }

  /**
   * Write <code>val</code> into <code>buff</code>, respecting
   * <code>offset</code> and <code>length</code>.
   */
  public void encode(ByteBuffer buff, byte[] val, int offset, int length) {
    buff.put(val, offset, length);
  }
}
