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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Numeric;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;

/**
 * An {@link Number} of arbitrary precision and variable-length encoding. The
 * resulting length of encoded values is determined by the numerical (base
 * 100) precision, not absolute value. Use this data type anywhere you would
 * expect to use a <code>DECIMAL</code> type, a {@link BigDecimal}, a
 * {@link BigInteger}, or any time you've parsed floating precision values
 * from text.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedNumeric extends OrderedBytesBase<Numeric> {

  public static final OrderedNumeric ASCENDING = new OrderedNumeric(Order.ASCENDING);
  public static final OrderedNumeric DESCENDING = new OrderedNumeric(Order.DESCENDING);

  protected OrderedNumeric(Order order) { super(order); }

  @Override
  public int encodedLength(Numeric val) {
    // TODO: this could be done better.
    ByteBuffer buff = ByteBuffer.allocate(100);
    encode(buff, val);
    return buff.position();
  }

  @Override
  public Class<Numeric> encodedClass() { return Numeric.class; }

  @Override
  public Numeric decode(ByteBuffer buff) {
    return OrderedBytes.decodeNumeric(buff);
  }

  @Override
  public void encode(ByteBuffer b, Numeric val) {
    if (null == val) {
      OrderedBytes.encodeNull(b, order);
    } else if (val.isInteger()) {
      OrderedBytes.encodeNumeric(b, val.longValue(), order);
    } else if (val.isReal()) {
      OrderedBytes.encodeNumeric(b, val.doubleValue(), order);
    } else {
      OrderedBytes.encodeNumeric(b, val.exactValue(), order);
    }
  }

  /**
   * Read a <code>long</code> value from the buffer <code>buff</code>.
   */
  public long decodeLong(ByteBuffer buff) {
    return OrderedBytes.decodeNumeric(buff).longValue();
  }

  /**
   * Write instance <code>val</code> into buffer <code>buff</code>.
   */
  public void encodeLong(ByteBuffer buff, long val) {
    OrderedBytes.encodeNumeric(buff, val, order);
  }

  /**
   * Read a <code>double</code> value from the buffer <code>buff</code>.
   */
  public double decodeDouble(ByteBuffer buff) {
    return OrderedBytes.decodeNumeric(buff).doubleValue();
  }

  /**
   * Write instance <code>val</code> into buffer <code>buff</code>.
   */
  public void encodeDouble(ByteBuffer buff, double val) {
    OrderedBytes.encodeNumeric(buff, val, order);
  }
}
