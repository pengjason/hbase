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
import org.apache.hadoop.hbase.util.OrderedBytes;


/**
 * A <code>long</code> of 64-bits using a fixed-length encoding.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedInt64 extends OrderedBytesBase<Long> {

  public static final OrderedInt64 ASCENDING = new OrderedInt64(Order.ASCENDING);
  public static final OrderedInt64 DESCENDING = new OrderedInt64(Order.DESCENDING);

  protected OrderedInt64(Order order) { super(order); }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public int encodedLength(Long val) { return 9; }

  @Override
  public Class<Long> encodedClass() { return Long.class; }

  @Override
  public Long decode(ByteBuffer buff) {
    return OrderedBytes.decodeInt64(buff);
  }

  @Override
  public void encode(ByteBuffer buff, Long val) {
    if (null == val) throw new IllegalArgumentException("Null values not supported.");
    OrderedBytes.encodeInt64(buff, val, order);
  }

  /**
   * Read a <code>long</code> value from the buffer <code>buff</code>.
   */
  public long decodeLong(ByteBuffer buff) {
    return OrderedBytes.decodeInt64(buff);
  }

  /**
   * Write instance <code>val</code> into buffer <code>buff</code>.
   */
  public void encodeLong(ByteBuffer buff, long val) {
    OrderedBytes.encodeInt64(buff, val, order);
  }
}
