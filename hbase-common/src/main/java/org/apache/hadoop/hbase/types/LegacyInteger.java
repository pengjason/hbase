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
 * {@link Bytes#putInt(byte[], int, int)}. Intended to make it easier to
 * transition away from direct use of {@link Bytes}.
 * @see Bytes#putInt(byte[], int, int)
 * @see Bytes#toInt(byte[])
 * @see OrderedInt32
 * @see OrderedNumeric
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LegacyInteger implements DataType<Integer> {

  @Override
  public boolean isOrderPreserving() { return false; }

  @Override
  public Order getOrder() { return null; }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public boolean isSkippable() { return true; }

  @Override
  public int encodedLength(Integer val) { return Bytes.SIZEOF_INT; }

  @Override
  public Class<Integer> encodedClass() { return Integer.class; }

  @Override
  public void skip(ByteBuffer buff) {
    buff.position(buff.position() + Bytes.SIZEOF_INT);
  }

  @Override
  public Integer decode(ByteBuffer buff) {
    int val = Bytes.toInt(buff.array(), buff.position());
    skip(buff);
    return val;
  }

  @Override
  public void encode(ByteBuffer buff, Integer val) {
    Bytes.putInt(buff.array(), buff.position(), val);
    skip(buff);
  }

  /**
   * Read an <code>int</code> value from the buffer <code>buff</code>.
   */
  public int decodeInt(ByteBuffer buff) {
    int val = Bytes.toInt(buff.array(), buff.position());
    skip(buff);
    return val;
  }

  /**
   * Write instance <code>val</code> into buffer <code>buff</code>.
   */
  public void encodeInt(ByteBuffer buff, int val) {
    Bytes.putInt(buff.array(), buff.position(), val);
    skip(buff);
  }
}
