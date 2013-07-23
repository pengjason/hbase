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
 * An <code>HDataType</code> that encodes variable-length values encoded using
 * {@link Bytes#toBytes(String)}. Includes a termination marker following the
 * raw <code>byte[]</code> value. Intended to make it easier to transition
 * away from direct use of {@link Bytes}.
 * @see Bytes#toBytes(String)
 * @see Bytes#toString(byte[], int, int)
 * @see LegacyString
 * @see OrderedString
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LegacyStringTerminated extends TerminatedWrapper<String> {

  /**
   * Create a <code>LegacyStringTerminated</code> using the specified terminator and
   * <code>order</code>.
   * @throws IllegalArgumentException if <code>term</code> is <code>null</code> or empty.
   */
  public LegacyStringTerminated(Order order, byte[] term) {
    super(new LegacyString(order), term);
  }

  /**
   * Create a <code>LegacyStringTerminated</code> using the specified terminator and
   * <code>order</code>.
   * @throws IllegalArgumentException if <code>term</code> is <code>null</code> or empty.
   */
  public LegacyStringTerminated(Order order, String term) {
    super(new LegacyString(order), term);
  }

  /**
   * Create a <code>LegacyStringTerminated</code> using the specified terminator.
   * @throws IllegalArgumentException if <code>term</code> is <code>null</code> or empty.
   */
  public LegacyStringTerminated(byte[] term) {
    super(new LegacyString(), term);
  }

  /**
   * Create a <code>LegacyStringTerminated</code> using the specified terminator.
   * @throws IllegalArgumentException if <code>term</code> is <code>null</code> or empty.
   */
  public LegacyStringTerminated(String term) {
    super(new LegacyString(), term);
  }

  /**
   * Read a <code>String</code> from the buffer <code>buff</code>.
   */
  public String decode(ByteBuffer buff, int length) {
    return ((LegacyString) wrapped).decode(buff, length);
  }

  /**
   * Write a <code>String</code> into <code>buff</code> at position
   * <code>offset</code>.
   * @return incremented offset.
   */
  public int encode(byte[] buff, int offset, String val) {
    return ((LegacyString) wrapped).encode(buff, offset, val);
  }
}
