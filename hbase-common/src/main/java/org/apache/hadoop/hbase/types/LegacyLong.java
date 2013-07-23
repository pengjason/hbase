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
 * {@link Bytes#putLong(byte[], int, long)}. Intended to make it easier to
 * transition away from direct use of {@link Bytes}.
 * @see Bytes#putLong(byte[], int, long)
 * @see Bytes#toLong(byte[])
 * @see OrderedInt64
 * @see OrderedNumeric
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LegacyLong implements DataType<Long> {

  @Override
  public boolean isOrderPreserving() { return false; }

  @Override
  public Order getOrder() { return null; }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public boolean isSkippable() { return true; }

  @Override
  public int encodedLength(Long val) { return Bytes.SIZEOF_LONG; }

  @Override
  public Class<Long> encodedClass() { return Long.class; }

  @Override
  public void skip(ByteBuffer buff) {
    buff.position(buff.position() + Bytes.SIZEOF_LONG);
  }

  @Override
  public Long decode(ByteBuffer buff) {
    long val = Bytes.toLong(buff.array(), buff.position());
    skip(buff);
    return val;
  }

  @Override
  public void encode(ByteBuffer buff, Long val) {
    Bytes.putLong(buff.array(), buff.position(), val);
    skip(buff);
  }

  /**
   * Read a <code>long</code> value from the buffer <code>buff</code>.
   */
  public long decodeLong(ByteBuffer buff) {
    long val = Bytes.toLong(buff.array(), buff.position());
    skip(buff);
    return val;
  }

  /**
   * Write instance <code>val</code> into buffer <code>buff</code>.
   */
  public void encodeLong(ByteBuffer buff, long val) {
    Bytes.putLong(buff.array(), buff.position(), val);
    skip(buff);
  }
}
