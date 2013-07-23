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
 * {@link Bytes#putBytes(byte[], int, byte[], int, int)}. Includes a
 * termination marker following the raw <code>byte[]</code> value. Intended to
 * make it easier to transition away from direct use of {@link Bytes}.
 * @see Bytes#putBytes(byte[], int, byte[], int, int)
 * @see LegacyBytes
 * @see OrderedBlob
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LegacyBytesTerminated extends TerminatedWrapper<byte[]> {

  /**
   * Create a <code>LegacyBytesTerminated</code> using the specified terminator and
   * <code>order</code>.
   * @throws IllegalArgumentException if <code>term</code> is <code>null</code> or empty.
   */
  public LegacyBytesTerminated(Order order, byte[] term) {
    super(new LegacyBytes(order), term);
  }

  /**
   * Create a <code>LegacyBytesTerminated</code> using the specified terminator and
   * <code>order</code>.
   * @throws IllegalArgumentException if <code>term</code> is <code>null</code> or empty.
   */
  public LegacyBytesTerminated(Order order, String term) {
    super(new LegacyBytes(order), term);
  }

  /**
   * Create a <code>LegacyBytesTerminated</code> using the specified terminator.
   * @throws IllegalArgumentException if <code>term</code> is <code>null</code> or empty.
   */
  public LegacyBytesTerminated(byte[] term) {
    super(new LegacyBytes(), term);
  }

  /**
   * Create a <code>LegacyBytesTerminated</code> using the specified terminator.
   * @throws IllegalArgumentException if <code>term</code> is <code>null</code> or empty.
   */
  public LegacyBytesTerminated(String term) {
    super(new LegacyBytes(), term);
  }

  /**
   * Read a <code>byte[]</code> from the buffer <code>buff</code>.
   */
  public byte[] decode(ByteBuffer buff, int length) {
    return ((LegacyBytes) wrapped).decode(buff, length);
  }

  /**
   * Write <code>val</code> into <code>buff</code>, respecting
   * <code>offset</code> and <code>length</code>.
   */
  public void encode(ByteBuffer buff, byte[] val, int offset, int length) {
    ((LegacyBytes) wrapped).encode(buff, val, offset, length);
  }
}
