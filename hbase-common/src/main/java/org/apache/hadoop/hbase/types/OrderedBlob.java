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
 * A <code>byte[]</code> of variable-length.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedBlob extends OrderedBytesBase<byte[]> {

  public static final OrderedBlob ASCENDING = new OrderedBlob(Order.ASCENDING);
  public static final OrderedBlob DESCENDING = new OrderedBlob(Order.DESCENDING);

  protected OrderedBlob(Order order) { super(order); }

  @Override
  public boolean isSkippable() { return false; }

  @Override
  public int encodedLength(byte[] val) {
    return null == val ?
      (Order.ASCENDING == order ? 1 : 2) :
      (Order.ASCENDING == order ? val.length + 1 : val.length + 2);
  }

  @Override
  public Class<byte[]> encodedClass() { return byte[].class; }

  @Override
  public byte[] decode(ByteBuffer buff) {
    return OrderedBytes.decodeBlobCopy(buff);
  }

  @Override
  public void encode(ByteBuffer buff, byte[] val) {
    OrderedBytes.encodeBlobCopy(buff, val, order);
  }

  /**
   * Write a subset of <code>val</code> to <code>buff</code>.
   */
  public void encode(ByteBuffer buff, byte[] val, int length, int offset) {
    OrderedBytes.encodeBlobCopy(buff, val, offset, length, order);
  }
}
