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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.junit.Test;

public class TestUnion2 {

  /**
   * An example <code>Union</code>
   */
  private static class SampleUnion1 extends Union2<Integer, String> {

    private final Order ord;

    public SampleUnion1(Order order) {
      super(
        Order.ASCENDING == order ? OrderedInt32.ASCENDING : OrderedInt32.DESCENDING,
        Order.ASCENDING == order ? OrderedString.ASCENDING : OrderedString.DESCENDING);
      ord = order;
    }

    @Override
    public void skip(ByteBuffer buff) {
      // both types A and B support generic OrderedBytes.skip, so this is easy
      OrderedBytes.skip(buff);
    }

    @Override
    public Object decode(ByteBuffer buff) {
      byte header = ord.apply(buff.array()[buff.position()]);
      switch (header) {
        case OrderedBytes.FIXED_INT32:
          return typeA.decode(buff);
        case OrderedBytes.TEXT:
          return typeB.decode(buff);
        default:
          throw new IllegalArgumentException("Unrecognized encoding format.");
      }
    }

    @Override
    public int encodedLength(Object val) {
      Integer i = null;
      String s = null;
      try {
        i = (Integer) val;
      } catch (ClassCastException e) {}
      try {
        s = (String) val;
      } catch (ClassCastException e) {}

      if (null != i) return typeA.encodedLength(i);
      if (null != s) return typeB.encodedLength(s);
      throw new IllegalArgumentException("val is not a valid member of this union.");
    }

    @Override
    public void encode(ByteBuffer buff, Object val) {
      Integer i = null;
      String s = null;
      try {
        i = (Integer) val;
      } catch (ClassCastException e) {}
      try {
        s = (String) val;
      } catch (ClassCastException e) {}

      if (null != i) typeA.encode(buff, i);
      else if (null != s) typeB.encode(buff, s);
      else
        throw new IllegalArgumentException("val is not of a supported type.");
    }

  }

  @Test
  public void testEncodeDecode() {
    Integer intVal = Integer.valueOf(10);
    String strVal = "hello";
    ByteBuffer buff = ByteBuffer.allocate(10);

    for (SampleUnion1 type : new SampleUnion1[] {
        new SampleUnion1(Order.ASCENDING),
        new SampleUnion1(Order.DESCENDING)
    }) {
      buff.clear();
      type.encode(buff, intVal);
      buff.flip();
      assertTrue(0 == intVal.compareTo(type.decodeA(buff)));

      buff.clear();
      type.encode(buff, strVal);
      buff.flip();
      assertTrue(0 == strVal.compareTo(type.decodeB(buff)));
    }
  }

  @Test
  public void testSkip() {
    Integer intVal = Integer.valueOf(10);
    String strVal = "hello";
    ByteBuffer buff = ByteBuffer.allocate(10);

    for (SampleUnion1 type : new SampleUnion1[] {
        new SampleUnion1(Order.ASCENDING),
        new SampleUnion1(Order.DESCENDING)
    }) {
      buff.clear();
      type.encode(buff, intVal);
      buff.flip();
      type.skip(buff);
      assertEquals(buff.limit(), buff.position());

      buff.clear();
      type.encode(buff, strVal);
      buff.flip();
      type.skip(buff);
      assertEquals(buff.limit(), buff.position());
    }
  }
}
