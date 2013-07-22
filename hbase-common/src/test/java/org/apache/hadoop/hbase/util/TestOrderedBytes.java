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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestOrderedBytes {

  // integer constants for testing Numeric code paths
  static final Long[] I_VALS =
    { 0L, 1L, 10L, 99L, 100L, 1234L, 9999L, 10000L, 10001L, 12345L, 123450L, Long.MAX_VALUE };
  static final int[] I_LENGTHS = { 1, 2, 2, 2, 2, 3, 3, 2, 4, 4, 4, 11 };

  // real constants for testing Numeric code paths
  static final Double[] D_VALS =
    { 0.0, 0.00123, 0.0123, 0.123, 1.0, 10.0, 12.345, 99.0, 99.01, 99.0001, 100.0, 100.01,
      100.1, 1234.0, 1234.5, 9999.0, 9999.000001, 9999.000009, 9999.00001, 9999.00009,
      9999.000099, 9999.0001, 9999.001, 9999.01, 9999.1, 10000.0, 10001.0, 12345.0, 123450.0,
      Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN, Double.MAX_VALUE };
  static final int[] D_LENGTHS =
    { 1, 4, 4, 4, 2, 2, 4, 2, 3, 4, 2, 4,
      4, 3, 4, 3, 6, 6, 6, 6,
      6, 5, 5, 4, 4, 2, 4, 4, 4,
      1, 1, 1, 11 };

  // fill in other gaps in Numeric code paths
  static final Numeric[] N_VALS =
    { null, new Numeric(Long.MAX_VALUE), new Numeric(Long.MIN_VALUE),
      new Numeric(Double.MAX_VALUE), new Numeric(Double.MIN_VALUE),
      new Numeric(BigDecimal.valueOf(Long.MAX_VALUE).multiply(BigDecimal.valueOf(100))) };
  static final int[] N_LENGTHS =
    { 1, 11, 11, 11, 4, 12 };

  /*
   * This is the smallest difference between two doubles in D_VALS
   */
  static final double MIN_EPSILON = 0.000001;

  /**
   * Expected lengths of equivalent values should match
   */
  @Test
  public void testVerifyTestIntegrity() {
    for (int i = 0; i < I_VALS.length; i++) {
      for (int d = 0; d < D_VALS.length; d++) {
        if (Math.abs(I_VALS[i] - D_VALS[d]) < MIN_EPSILON) {
          assertEquals(
            "Test inconsistency detected: expected lengths for " + I_VALS[i] + " do not match.",
            I_LENGTHS[i], D_LENGTHS[d]);
        }
      }
    }
  }

  /**
   * Tests the variable uint64 encoding.
   * <p>
   * Building sqlite4 with -DVARINT_TOOL provides this reference:<br />
   * <code>$ ./varint_tool 240 2287 67823 16777215 4294967295 1099511627775
   *   281474976710655 72057594037927935 18446744073709551615<br />
   * 240 = f0<br />
   * 2287 = f8ff<br />
   * 67823 = f9ffff<br />
   * 16777215 = faffffff<br />
   * 4294967295 = fbffffffff<br />
   * 1099511627775 = fcffffffffff<br />
   * 281474976710655 = fdffffffffffff<br />
   * 72057594037927935 = feffffffffffffff<br />
   * 9223372036854775807 = ff7fffffffffffffff (Long.MAX_VAL)<br />
   * 9223372036854775808 = ff8000000000000000 (Long.MIN_VAL)<br />
   * 18446744073709551615 = ffffffffffffffffff<br /></code>
   * </p>
   */
  @Test
  public void testVaru64Boundaries() {
    int len;

    long vals[] =
        { 239L, 240L, 2286L, 2287L, 67822L, 67823L, 16777214L, 16777215L, 4294967294L, 4294967295L,
          1099511627774L, 1099511627775L, 281474976710654L, 281474976710655L, 72057594037927934L,
          72057594037927935L, Long.MAX_VALUE - 1, Long.MAX_VALUE, Long.MIN_VALUE + 1,
          Long.MIN_VALUE, -2L, -1L };
    int lens[] = { 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 9, 9, 9, 9 };
    assertEquals("Broken test!", vals.length, lens.length);

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (boolean comp : new boolean[] { true, false }) {
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf = ByteBuffer.allocate(lens[i] + 1);
        buf.get(); // skip first byte
        len = OrderedBytes.putVaruint64(buf, vals[i], comp);
        assertEquals("Surprising serialized length.", lens[i], len);
        assertEquals(buf.limit(), buf.position());
        buf.flip();
        buf.get(); // skip first byte
        assertEquals("Length inspection failed.",
          lens[i], OrderedBytes.lengthVaru64(buf, comp));
        assertEquals("Deserialization failed.", vals[i], OrderedBytes.getVaruint64(buf, comp));
        assertEquals(buf.limit(), buf.position());
      }
    }
  }

  /**
   * Test integer encoding. Example input values come from reference wiki
   * page.
   */
  protected void testNumericInt() {
    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < I_VALS.length; i++) {
        ByteBuffer buf1 = ByteBuffer.allocate(I_LENGTHS[i] + 1);
        buf1.get();
        OrderedBytes.encodeNumeric(buf1, I_VALS[i], ord);
        assertEquals(
          "Encoded value does not match expected length.",
          buf1.capacity(), buf1.position());
        buf1.flip();
        buf1.get();
        long decoded = OrderedBytes.decodeNumeric(buf1).longValue();
        assertEquals(
          "Decoded value does not match expected value.",
          I_VALS[i].longValue(), decoded);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[I_VALS.length][];
      for (int i = 0; i < I_VALS.length; i++) {
        ByteBuffer buf = ByteBuffer.allocate(I_LENGTHS[i] + 1);
        buf.get();
        OrderedBytes.encodeNumeric(buf, I_VALS[i], ord);
        encoded[i] = buf.array();
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Long[] sortedVals = Arrays.copyOf(I_VALS, I_VALS.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        ByteBuffer buf = ByteBuffer.wrap(encoded[i]);
        buf.get();
        long decoded = OrderedBytes.decodeNumeric(buf).longValue();
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
          sortedVals[i].longValue(), decoded);
      }
    }
  }

  /**
   * Test real encoding. Example input values come from reference wiki page.
   */
  protected void testNumericReal() {
    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < D_VALS.length; i++) {
        ByteBuffer buf1 = ByteBuffer.allocate(D_LENGTHS[i] + 1);
        buf1.get();
        OrderedBytes.encodeNumeric(buf1, D_VALS[i], ord);
        assertEquals(buf1.capacity(), buf1.position());
        buf1.flip();
        buf1.get();
        double decoded = OrderedBytes.decodeNumeric(buf1).doubleValue();
        assertEquals(
          "Decoded value does not match expected value.",
          D_VALS[i].doubleValue(),
          decoded, MIN_EPSILON);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[D_VALS.length][];
      for (int i = 0; i < D_VALS.length; i++) {
        ByteBuffer buf = ByteBuffer.allocate(D_LENGTHS[i] + 1);
        buf.get();
        OrderedBytes.encodeNumeric(buf, D_VALS[i], ord);
        encoded[i] = buf.array();
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Double[] sortedVals = Arrays.copyOf(D_VALS, D_VALS.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        ByteBuffer buf = ByteBuffer.wrap(encoded[i]);
        buf.get();
        double decoded = OrderedBytes.decodeNumeric(buf).doubleValue();
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
            sortedVals[i].doubleValue(), decoded, MIN_EPSILON);
      }
    }
  }

  /**
   * Fill gaps in Numeric encoding testing.
   */
  protected void testNumericOther() {
    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < N_VALS.length; i++) {
        ByteBuffer buf1 = ByteBuffer.allocate(N_LENGTHS[i] + 1);
        buf1.get();
        OrderedBytes.encodeNumeric(buf1, N_VALS[i], ord);
        assertEquals(buf1.capacity(), buf1.position());
        buf1.flip();
        buf1.get();
        Numeric decoded = OrderedBytes.decodeNumeric(buf1);
        assertEquals("Decoded value does not match expected value.", N_VALS[i], decoded);
      }
    }
  }

  /**
   * Verify Real and Int encodings are compatible.
   */
  protected void testNumericIntRealCompatibility() {
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < I_VALS.length; i++) {
        // skip values for which BigDecimal instantiation drops precision
        BigDecimal bdi = BigDecimal.valueOf(I_VALS[i]);
        if (bdi.compareTo(BigDecimal.valueOf((double) I_VALS[i])) != 0) continue;

        // verify primitives
        ByteBuffer bi = ByteBuffer.allocate(I_LENGTHS[i]);
        ByteBuffer br = ByteBuffer.allocate(I_LENGTHS[i]);
        OrderedBytes.encodeNumeric(bi, I_VALS[i], ord);
        OrderedBytes.encodeNumeric(br, I_VALS[i], ord);
        assertEquals(bi, br);
        bi.flip();
        assertEquals((long) I_VALS[i], OrderedBytes.decodeNumeric(bi).longValue());
        br.flip();
        assertEquals((long) I_VALS[i], (long) OrderedBytes.decodeNumeric(br).doubleValue());

        // verify BigDecimal for Real encoding
        br = ByteBuffer.allocate(I_LENGTHS[i]);
        OrderedBytes.encodeNumeric(br, bdi, ord);
        assertEquals(bi, br);
        bi.flip();
        assertEquals(0,
          bdi.compareTo(BigDecimal.valueOf(OrderedBytes.decodeNumeric(bi).longValue())));
      }
    }
  }

  /**
   * Test Numeric encoding.
   */
  @Test
  public void testNumeric() {
    testNumericInt();
    testNumericReal();
    testNumericOther();
    testNumericIntRealCompatibility();
  }

  /**
   * Test int32 encoding.
   */
  @Test
  public void testInt32() {
    Integer[] vals =
      { Integer.MIN_VALUE, Integer.MIN_VALUE / 2, 0, Integer.MAX_VALUE / 2, Integer.MAX_VALUE };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf1 = ByteBuffer.allocate(5 + 1);
        buf1.get();
        OrderedBytes.encodeInt32(buf1, vals[i], ord);
        assertEquals(
          "Encoded value does not match expected length.",
          buf1.capacity(), buf1.position());
        buf1.flip();
        buf1.get();
        int decoded = OrderedBytes.decodeInt32(buf1);
        assertEquals("Decoded value does not match expected value.",
          vals[i].intValue(), decoded);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][];
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf = ByteBuffer.allocate(5 + 1);
        buf.get();
        OrderedBytes.encodeInt32(buf, vals[i], ord);
        encoded[i] = buf.array();
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Integer[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        ByteBuffer buf = ByteBuffer.wrap(encoded[i]);
        buf.get();
        int decoded = OrderedBytes.decodeInt32(buf);
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
          sortedVals[i].intValue(), decoded);
      }
    }
  }

  /**
   * Test int64 encoding.
   */
  @Test
  public void testInt64() {
    Long[] vals = { Long.MIN_VALUE, Long.MIN_VALUE / 2, 0L, Long.MAX_VALUE / 2, Long.MAX_VALUE };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf1 = ByteBuffer.allocate(9 + 1);
        buf1.get();
        OrderedBytes.encodeInt64(buf1, vals[i], ord);
        assertEquals("Encoded value does not match expected length.", buf1.capacity(),
          buf1.position());
        buf1.flip();
        buf1.get();
        long decoded = OrderedBytes.decodeInt64(buf1);
        assertEquals("Decoded value does not match expected value.", vals[i].longValue(), decoded);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][];
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf = ByteBuffer.allocate(9 + 1);
        buf.get();
        OrderedBytes.encodeInt64(buf, vals[i], ord);
        encoded[i] = buf.array();
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Long[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        ByteBuffer buf = ByteBuffer.wrap(encoded[i]);
        buf.get();
        long decoded = OrderedBytes.decodeInt64(buf);
        assertEquals(String.format(
          "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
          sortedVals[i].longValue(), decoded);
      }
    }
  }

  /**
   * Test float32 encoding.
   */
  @Test
  public void testFloat32() {
    Float[] vals =
      { Float.MIN_VALUE, Float.MIN_VALUE + 1.0f, 0.0f, Float.MAX_VALUE / 2.0f, Float.MAX_VALUE };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf1 = ByteBuffer.allocate(5 + 1);
        buf1.get();
        OrderedBytes.encodeFloat32(buf1, vals[i], ord);
        assertEquals("Encoded value does not match expected length.", buf1.capacity(),
          buf1.position());
        buf1.flip();
        buf1.get();
        float decoded = OrderedBytes.decodeFloat32(buf1);
        assertEquals("Decoded value does not match expected value.",
          Float.floatToIntBits(vals[i].floatValue()),
          Float.floatToIntBits(decoded));
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][];
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf = ByteBuffer.allocate(5 + 1);
        buf.get();
        OrderedBytes.encodeFloat32(buf, vals[i], ord);
        encoded[i] = buf.array();
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Float[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        ByteBuffer buf = ByteBuffer.wrap(encoded[i]);
        buf.get();
        float decoded = OrderedBytes.decodeFloat32(buf);
        assertEquals(String.format(
          "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
          Float.floatToIntBits(sortedVals[i].floatValue()),
          Float.floatToIntBits(decoded));
      }
    }
  }

  /**
   * Test float64 encoding.
   */
  @Test
  public void testFloat64() {
    Double[] vals =
      { Double.MIN_VALUE, Double.MIN_VALUE + 1.0, 0.0, Double.MAX_VALUE / 2.0, Double.MAX_VALUE };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf1 = ByteBuffer.allocate(9 + 1);
        buf1.get();
        OrderedBytes.encodeFloat64(buf1, vals[i], ord);
        assertEquals("Encoded value does not match expected length.", buf1.capacity(),
          buf1.position());
        buf1.flip();
        buf1.get();
        double decoded = OrderedBytes.decodeFloat64(buf1);
        assertEquals("Decoded value does not match expected value.",
          Double.doubleToLongBits(vals[i].doubleValue()),
          Double.doubleToLongBits(decoded));
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][];
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf = ByteBuffer.allocate(9 + 1);
        buf.get();
        OrderedBytes.encodeFloat64(buf, vals[i], ord);
        encoded[i] = buf.array();
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Double[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        ByteBuffer buf = ByteBuffer.wrap(encoded[i]);
        buf.get();
        double decoded = OrderedBytes.decodeFloat64(buf);
        assertEquals(String.format(
          "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
          Double.doubleToLongBits(sortedVals[i].doubleValue()),
          Double.doubleToLongBits(decoded));
      }
    }
  }

  /**
   * Test string encoding.
   */
  @Test
  public void testString() {
    String[] vals = { "foo", "bar", "baz" };
    int expectedLengths[] = { 5, 5, 5 };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf1 = ByteBuffer.allocate(expectedLengths[i] + 1);
        buf1.get();
        OrderedBytes.encodeString(buf1, vals[i], ord);
        buf1.flip();
        buf1.get();
        assertEquals(
          "Decoded value does not match expected value.",
          vals[i], OrderedBytes.decodeString(buf1));
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][];
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf = ByteBuffer.allocate(expectedLengths[i] + 1);
        buf.get();
        OrderedBytes.encodeString(buf, vals[i], ord);
        encoded[i] = buf.array();
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      String[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        ByteBuffer buf = ByteBuffer.wrap(encoded[i]);
        buf.get();
        String decoded = OrderedBytes.decodeString(buf);
        assertEquals(String.format(
          "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
          sortedVals[i], decoded);
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStringNoNullChars() {
    ByteBuffer buff = ByteBuffer.allocate(3);
    OrderedBytes.encodeString(buff, "\u0000", Order.ASCENDING);
  }

  /**
   * Test length estimation algorithms for Blob-mid encoding. Does not cover
   * 0-length input case properly.
   */
  @Test
  public void testblobMidLencodedLength() {
    int[][] values = {
        /* decoded length, encoded length
         * ceil((n bytes * 8 bits/input byte) / 7 bits/encoded byte) + 1 header
         */
        { 1, 3 }, { 2, 4 }, { 3, 5 }, { 4, 6 },
        { 5, 7 }, { 6, 8 }, { 7, 9 }, { 8, 11 }
      };

    for (int[] pair : values) {
      assertEquals(pair[1], OrderedBytes.blobVarEncodedLength(pair[0]));
      assertEquals(pair[0], OrderedBytes.blobVarDecodedLength(pair[1]));
    }
  }

  /**
   * Test Blob-mid encoding.
   */
  @Test
  public void testBlobMid() {
    byte[][] vals =
        { "".getBytes(), "foo".getBytes(), "foobarbazbub".getBytes(),
          { (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa,
            (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa },
          { (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
            (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55 },
          "1".getBytes(), "22".getBytes(), "333".getBytes(), "4444".getBytes(),
          "55555".getBytes(), "666666".getBytes(), "7777777".getBytes(), "88888888".getBytes()
        };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (byte[] val : vals) {
        ByteBuffer buf1 = ByteBuffer.allocate(OrderedBytes.blobVarEncodedLength(val.length) + 1);
        buf1.get();
        OrderedBytes.encodeBlobVar(buf1, val, ord);
        buf1.flip();
        buf1.get();
        byte[] decoded = OrderedBytes.decodeBlobVar(buf1);
        assertArrayEquals("Decoded value does not match expected value.", val, decoded);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][];
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf = ByteBuffer.allocate(OrderedBytes.blobVarEncodedLength(vals[i].length) + 1);
        buf.get();
        OrderedBytes.encodeBlobVar(buf, vals[i], ord);
        encoded[i] = buf.array();
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      byte[][] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals, Bytes.BYTES_COMPARATOR);
      else Arrays.sort(sortedVals, Collections.reverseOrder(Bytes.BYTES_COMPARATOR));

      for (int i = 0; i < sortedVals.length; i++) {
        ByteBuffer buf = ByteBuffer.wrap(encoded[i]);
        buf.get();
        byte[] decoded = OrderedBytes.decodeBlobVar(buf);
        assertArrayEquals(String.format(
          "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
          sortedVals[i], decoded);
      }
    }
  }

  /**
   * Test Blob-last encoding.
   */
  @Test
  public void testBlobLast() {
    byte[][] vals =
      { "".getBytes(), "foo".getBytes(), "foobarbazbub".getBytes(),
        { (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa,
          (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa },
        { (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
          (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55 },
      };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (byte[] val : vals) {
        ByteBuffer buf1 = ByteBuffer.allocate(val.length + (ord == Order.ASCENDING ? 2 : 3));
        buf1.get();
        OrderedBytes.encodeBlobCopy(buf1, val, ord);
        buf1.flip();
        buf1.get();
        assertArrayEquals(
          "Decoded value does not match expected value.",
          val, OrderedBytes.decodeBlobCopy(buf1));
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][];
      for (int i = 0; i < vals.length; i++) {
        ByteBuffer buf = ByteBuffer.allocate(vals[i].length + (ord == Order.ASCENDING ? 2 : 3));
        buf.get();
        OrderedBytes.encodeBlobCopy(buf, vals[i], ord);
        encoded[i] = buf.array();
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      byte[][] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals, Bytes.BYTES_COMPARATOR);
      else Arrays.sort(sortedVals, Collections.reverseOrder(Bytes.BYTES_COMPARATOR));

      for (int i = 0; i < sortedVals.length; i++) {
        ByteBuffer buf = ByteBuffer.wrap(encoded[i]);
        buf.get();
        byte[] decoded = OrderedBytes.decodeBlobCopy(buf);
        assertArrayEquals(String.format(
          "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
          sortedVals[i], decoded);
      }
    }

    /*
     * assert byte[] segments are serialized correctly.
     */
    ByteBuffer buf = ByteBuffer.allocate(3 + 2);
    OrderedBytes.encodeBlobCopy(buf, "foobarbaz".getBytes(), 3, 3, Order.ASCENDING);
    buf.flip();
    assertArrayEquals("bar".getBytes(), OrderedBytes.decodeBlobCopy(buf));
  }

  /**
   * Assert invalid input byte[] are rejected by Blob-last
   */
  @Test(expected = IllegalArgumentException.class)
  public void testBlobLastNoZeroBytes() {
    byte[] val = { 0x01, 0x02, 0x00, 0x03 };
    ByteBuffer buf = ByteBuffer.allocate(val.length + 2);
    OrderedBytes.encodeBlobCopy(buf, val, Order.ASCENDING);
    fail("test should never get here.");
  }

  /**
   * Test generic skip logic
   */
  @Test
  public void testSkip() {
    BigDecimal longMax = BigDecimal.valueOf(Long.MAX_VALUE);
    double negInf = Double.NEGATIVE_INFINITY;
    BigDecimal negLarge = longMax.multiply(longMax).negate();
    BigDecimal negMed = new BigDecimal("-10.0");
    BigDecimal negSmall = new BigDecimal("-0.0010");
    long zero = 0l;
    BigDecimal posSmall = negSmall.negate();
    BigDecimal posMed = negMed.negate();
    BigDecimal posLarge = negLarge.negate();
    double posInf = Double.POSITIVE_INFINITY;
    double nan = Double.NaN;
    int int32 = 100;
    long int64 = 100l;
    float float32 = 100.0f;
    double float64 = 100.0d;
    String text = "hello world.";
    byte[] blobMid = Bytes.toBytes("foo");
    byte[] blobLast = Bytes.toBytes("bar");

    ByteBuffer buff = ByteBuffer.allocate(30);
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      buff.clear();
      OrderedBytes.encodeNull(buff, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeNumeric(buff, negInf, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeNumeric(buff, negLarge, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeNumeric(buff, negMed, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeNumeric(buff, negSmall, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeNumeric(buff, zero, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeNumeric(buff, posSmall, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeNumeric(buff, posMed, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeNumeric(buff, posLarge, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeNumeric(buff, posInf, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeNumeric(buff, nan, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeInt32(buff, int32, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeInt64(buff, int64, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeFloat32(buff, float32, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeFloat64(buff, float64, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeString(buff, text, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeBlobVar(buff, blobMid, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());

      buff.clear();
      OrderedBytes.encodeBlobCopy(buff, blobLast, ord);
      buff.flip();
      OrderedBytes.skip(buff);
      assertEquals(buff.position(), buff.limit());
    }
  }
}
