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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestFixedLengthWrapper {

  static final byte[][] VALUES = new byte[][] {
    Bytes.toBytes(""), Bytes.toBytes("1"), Bytes.toBytes("22"), Bytes.toBytes("333"),
    Bytes.toBytes("4444"), Bytes.toBytes("55555"), Bytes.toBytes("666666"),
    Bytes.toBytes("7777777"), Bytes.toBytes("88888888"), Bytes.toBytes("999999999"),
  };

  /**
   * all values of <code>limit</code> are >= max length of a member of
   * <code>VALUES</code>.
   */
  static final int[] limits = { 9, 12, 15 };

  @Test
  public void testReadWrite() {
    for (int limit : limits) {
      ByteBuffer buff = ByteBuffer.allocate(limit);
      for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
        for (byte[] val : VALUES) {
          DataType<byte[]> type = new FixedLengthWrapper<byte[]>(new LegacyBytes(ord), limit);
          buff.clear();
          type.encode(buff, val);
          assertEquals(buff.limit(), buff.position());
          buff.flip();
          byte[] expected = Arrays.copyOf(val, limit);
          byte[] actual = type.decode(buff);
          assertEquals(buff.limit(), buff.position());
          assertArrayEquals(expected, actual);
        }
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsufficientRemainingRead() {
    ByteBuffer buff = ByteBuffer.allocate(0);
    DataType<byte[]> type = new FixedLengthWrapper<byte[]>(new LegacyBytes(), 3);
    type.decode(buff);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsufficientRemainingWrite() {
    ByteBuffer buff = ByteBuffer.allocate(0);
    DataType<byte[]> type = new FixedLengthWrapper<byte[]>(new LegacyBytes(), 3);
    type.encode(buff, Bytes.toBytes(""));
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflowPassthrough() {
    ByteBuffer buff = ByteBuffer.allocate(3);
    DataType<byte[]> type = new FixedLengthWrapper<byte[]>(new LegacyBytes(), 0);
    type.encode(buff, Bytes.toBytes("foo"));
  }
}
