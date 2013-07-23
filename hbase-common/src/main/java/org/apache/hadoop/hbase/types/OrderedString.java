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
 * A <code>String</code> of variable-length.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedString extends OrderedBytesBase<String> {

  public static final OrderedString ASCENDING = new OrderedString(Order.ASCENDING);
  public static final OrderedString DESCENDING = new OrderedString(Order.DESCENDING);

  protected OrderedString(Order order) { super(order); }

  @Override
  public int encodedLength(String val) {
    return null == val ? 1 : val.getBytes(OrderedBytes.UTF8).length + 2;
  }

  @Override
  public Class<String> encodedClass() { return String.class; }

  @Override
  public String decode(ByteBuffer buff) {
    return OrderedBytes.decodeString(buff);
  }

  @Override
  public void encode(ByteBuffer buff, String val) {
    OrderedBytes.encodeString(buff, val, order);
  }
}
