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
 * An <code>HDataType</code> for interacting with values encoded using
 * {@link Bytes#toBytes(String)}. Intended to make it easier to transition
 * away from direct use of {@link Bytes}.
 * @see Bytes#toBytes(String)
 * @see Bytes#toString(byte[])
 * @see LegacyStringTerminated
 * @see OrderedString
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LegacyString implements DataType<String> {

  protected final Order order;

  public LegacyString() { order = Order.ASCENDING; }
  public LegacyString(Order order) { this.order = order; }

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
  public int encodedLength(String val) { return Bytes.toBytes(val).length; }

  @Override
  public Class<String> encodedClass() { return String.class; }

  /**
   * Skip <code>buff</code> ahead <code>length</code> bytes.
   */
  public void skip(ByteBuffer buff, int length) {
    buff.position(buff.position() + length);
  }

  @Override
  public String decode(ByteBuffer buff) {
    return decode(buff, buff.limit() - buff.position());
  }

  @Override
  public void encode(ByteBuffer buff, String val) {
    buff.put(Bytes.toBytes(val));
  }

  /**
   * Read a <code>String</code> from the buffer <code>buff</code>.
   */
  public String decode(ByteBuffer buff, int length) {
    String val = Bytes.toString(buff.array(), buff.position(), length);
    skip(buff, length);
    return val;
  }

  /**
   * Write a <code>String</code> into <code>buff</code> at position
   * <code>offset</code>.
   * @return incremented offset.
   */
  public int encode(byte[] buff, int offset, String val) {
    byte[] s = Bytes.toBytes(val);
    return Bytes.putBytes(buff, offset, s, 0, s.length);
  }
}
