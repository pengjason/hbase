package org.apache.hadoop.hbase.types;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * This class both tests and demonstrates how to construct compound rowkeys
 * from a POJO. The code under test is {@link Struct}.
 * {@link SpecializedPojo1Type1} demonstrates how one might create their own
 * custom data type extension for an application POJO.
 */
@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestStruct {

  private Struct generic;
  @SuppressWarnings("rawtypes")
  private DataType specialized;
  private Object[][] constructorArgs;

  public TestStruct(Struct generic, @SuppressWarnings("rawtypes") DataType specialized,
      Object[][] constructorArgs) {
    this.generic = generic;
    this.specialized = specialized;
    this.constructorArgs = constructorArgs;
  }

  @Parameters
  public static Collection<Object[]> params() {
    Object[][] pojo1Type1Args = {
        new Object[] { "foo", -5,  new org.apache.hadoop.hbase.util.Numeric(10.001) },
        new Object[] { "foo", 100, new org.apache.hadoop.hbase.util.Numeric(-7.0)   },
        new Object[] { "foo", 100, new org.apache.hadoop.hbase.util.Numeric(10.001) },
        new Object[] { "bar", -5,  new org.apache.hadoop.hbase.util.Numeric(10.001) },
        new Object[] { "bar", 100, new org.apache.hadoop.hbase.util.Numeric(10.001) },
        new Object[] { "baz", -5,  new org.apache.hadoop.hbase.util.Numeric(10.001) },
    };

    Object[][] pojo1Type2Args = {
        new Object[] { "foo", new org.apache.hadoop.hbase.util.Numeric(-5),  10.001 },
        new Object[] { "foo", new org.apache.hadoop.hbase.util.Numeric(100), -7.0   },
        new Object[] { "foo", new org.apache.hadoop.hbase.util.Numeric(100), 10.001 },
        new Object[] { "bar", new org.apache.hadoop.hbase.util.Numeric(-5),  10.001 },
        new Object[] { "bar", new org.apache.hadoop.hbase.util.Numeric(100), 10.001 },
        new Object[] { "baz", new org.apache.hadoop.hbase.util.Numeric(-5),  10.001 },
    };

    Object[][] pojo2Type1Args = {
        new Object[] { null, "it".getBytes(), "was", "the".getBytes() },
        new Object[] { "best".getBytes(), null, "of", "times,".getBytes() },
        new Object[] { "it".getBytes(), "was".getBytes(), null, "the".getBytes() },
        new Object[] { "worst".getBytes(), "of".getBytes(), "times,", null },
        new Object[] { null, null, null, null },
    };

    Object[][] params = new Object[][] {
        { SpecializedPojo1Type1.GENERIC, new SpecializedPojo1Type1(), pojo1Type1Args },
        { SpecializedPojo1Type2.GENERIC, new SpecializedPojo1Type2(), pojo1Type2Args },
        { SpecializedPojo2Type1.GENERIC, new SpecializedPojo2Type1(), pojo2Type1Args },
    };
    return Arrays.asList(params);
  }

  static final Comparator<byte[]> NULL_SAFE_BYTES_COMPARATOR =
      new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
          if (o1 == o2) return 0;
          if (null == o1) return -1;
          if (null == o2) return 1;
          return Bytes.compareTo(o1, o2);
        }
      };

  /**
   * A simple object to serialize.
   */
  private static class Pojo1 implements Comparable<Pojo1> {
    final String stringFieldAsc;
    final int intFieldDsc;
    final double doubleFieldAsc;
    final transient String str;

    public Pojo1(Object... argv) {
      stringFieldAsc = (String) argv[0];
      intFieldDsc =
          argv[1] instanceof Integer ?
              (Integer) argv[1] :
              ((org.apache.hadoop.hbase.util.Numeric) argv[1]).intValue();
      doubleFieldAsc =
          argv[2] instanceof Double ?
              (Double) argv[2] :
              ((org.apache.hadoop.hbase.util.Numeric) argv[2]).doubleValue();
      str = new StringBuilder()
            .append("{ ")
            .append(null == stringFieldAsc ? "" : "\"")
            .append(stringFieldAsc)
            .append(null == stringFieldAsc ? "" : "\"").append(", ")
            .append(intFieldDsc).append(", ")
            .append(doubleFieldAsc)
            .append(" }")
            .toString();
    }

    @Override
    public String toString() {
      return str;
    }

    @Override
    public int compareTo(Pojo1 o) {
      int cmp = stringFieldAsc.compareTo(o.stringFieldAsc);
      if (cmp != 0) return cmp;
      cmp = -Integer.valueOf(intFieldDsc).compareTo(Integer.valueOf(o.intFieldDsc));
      if (cmp != 0) return cmp;
      return Double.compare(doubleFieldAsc, o.doubleFieldAsc);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (null == o) return false;
      if (!(o instanceof Pojo1)) return false;
      Pojo1 that = (Pojo1) o;
      return 0 == this.compareTo(that);
    }
  }

  /**
   * A simple object to serialize.
   */
  private static class Pojo2 implements Comparable<Pojo2> {
    final byte[] byteField1Asc;
    final byte[] byteField2Dsc;
    final String stringFieldDsc;
    final byte[] byteField3Dsc;
    final transient String str;

    public Pojo2(Object... vals) {
      byteField1Asc = (byte[]) vals[0];
      byteField2Dsc = (byte[]) vals[1];
      stringFieldDsc = (String) vals[2];
      byteField3Dsc = (byte[]) vals[3];
      str = new StringBuilder()
            .append("{ ")
            .append(Bytes.toStringBinary(byteField1Asc)).append(", ")
            .append(Bytes.toStringBinary(byteField2Dsc)).append(", ")
            .append(null == stringFieldDsc ? "" : "\"")
            .append(stringFieldDsc)
            .append(null == stringFieldDsc ? "" : "\"").append(", ")
            .append(Bytes.toStringBinary(byteField3Dsc))
            .append(" }")
            .toString();
    }

    @Override
    public String toString() {
      return str;
    }

    @Override
    public int compareTo(Pojo2 o) {
      int cmp = NULL_SAFE_BYTES_COMPARATOR.compare(byteField1Asc, o.byteField1Asc);
      if (cmp != 0) return cmp;
      cmp = -NULL_SAFE_BYTES_COMPARATOR.compare(byteField2Dsc, o.byteField2Dsc);
      if (cmp != 0) return cmp;
      if (stringFieldDsc == o.stringFieldDsc) cmp = 0;
      else if (null == stringFieldDsc) cmp = 1;
      else if (null == o.stringFieldDsc) cmp = -1;
      else cmp = -stringFieldDsc.compareTo(o.stringFieldDsc);
      if (cmp != 0) return cmp;
      return -NULL_SAFE_BYTES_COMPARATOR.compare(byteField3Dsc, o.byteField3Dsc);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (null == o) return false;
      if (!(o instanceof Pojo2)) return false;
      Pojo2 that = (Pojo2) o;
      return 0 == this.compareTo(that);
    }
  }

  /**
   * A custom data type implementation specialized for {@link Pojo1}.
   */
  private static class SpecializedPojo1Type1 implements DataType<Pojo1> {

    private static final OrderedString stringField = OrderedString.ASCENDING;
    private static final OrderedInt32 intField = OrderedInt32.DESCENDING;
    private static final OrderedNumeric doubleField = OrderedNumeric.ASCENDING;

    /**
     * The {@link Struct} equivalent of this type.
     */
    public static Struct GENERIC =
        new StructBuilder().add(stringField)
                           .add(intField)
                           .add(doubleField)
                           .toStruct();

    @Override
    public boolean isOrderPreserving() { return true; }

    @Override
    public Order getOrder() { return null; }

    @Override
    public boolean isNullable() { return false; }

    @Override
    public boolean isSkippable() { return true; }

    @Override
    public int encodedLength(Pojo1 val) {
      return
          stringField.encodedLength(val.stringFieldAsc) +
          intField.encodedLength(val.intFieldDsc) +
          doubleField.encodedLength(new org.apache.hadoop.hbase.util.Numeric(val.doubleFieldAsc));
    }

    @Override
    public Class<Pojo1> encodedClass() { return Pojo1.class; }

    @Override
    public void skip(ByteBuffer buff) {
      stringField.skip(buff);
      intField.skip(buff);
      doubleField.skip(buff);
    }

    @Override
    public Pojo1 decode(ByteBuffer buff) {
      return new Pojo1(new Object[] {
        stringField.decode(buff),
        intField.decodeInt(buff),
        doubleField.decodeDouble(buff)
      });
    }

    @Override
    public void encode(ByteBuffer buff, Pojo1 val) {
      stringField.encode(buff, val.stringFieldAsc);
      intField.encodeInt(buff, val.intFieldDsc);
      doubleField.encodeDouble(buff, val.doubleFieldAsc);
    }
  }

  /**
   * A custom data type implementation specialized for {@link Pojo1}.
   */
  private static class SpecializedPojo1Type2 implements DataType<Pojo1> {

    private static final OrderedString stringField = OrderedString.ASCENDING;
    private static final OrderedNumeric intField = OrderedNumeric.DESCENDING;
    private static final OrderedFloat64 doubleField = OrderedFloat64.ASCENDING;

    /**
     * The {@link Struct} equivalent of this type.
     */
    public static Struct GENERIC =
        new StructBuilder().add(stringField)
                           .add(intField)
                           .add(doubleField)
                           .toStruct();

    @Override
    public boolean isOrderPreserving() { return true; }

    @Override
    public Order getOrder() { return null; }

    @Override
    public boolean isNullable() { return false; }

    @Override
    public boolean isSkippable() { return true; }

    @Override
    public int encodedLength(Pojo1 val) {
      return
          stringField.encodedLength(val.stringFieldAsc) +
          intField.encodedLength(new org.apache.hadoop.hbase.util.Numeric(val.intFieldDsc)) +
          doubleField.encodedLength(val.doubleFieldAsc);
    }

    @Override
    public Class<Pojo1> encodedClass() { return Pojo1.class; }

    @Override
    public void skip(ByteBuffer buff) {
      stringField.skip(buff);
      intField.skip(buff);
      doubleField.skip(buff);
    }

    @Override
    public Pojo1 decode(ByteBuffer buff) {
      return new Pojo1(new Object[] {
        stringField.decode(buff),
        intField.decode(buff).intValue(),
        doubleField.decodeDouble(buff)
      });
    }

    @Override
    public void encode(ByteBuffer buff, Pojo1 val) {
      stringField.encode(buff, val.stringFieldAsc);
      intField.encodeLong(buff, val.intFieldDsc);
      doubleField.encodeDouble(buff, val.doubleFieldAsc);
    }
  }

  /**
   * A custom data type implementation specialized for {@link Pojo2}.
   */
  private static class SpecializedPojo2Type1 implements DataType<Pojo2> {

    private static OrderedBlobVar byteField1 = OrderedBlobVar.ASCENDING;
    private static OrderedBlobVar byteField2 = OrderedBlobVar.DESCENDING;
    private static OrderedString stringField = OrderedString.DESCENDING;
    private static OrderedBlob byteField3 = OrderedBlob.DESCENDING;

    /**
     * The {@link Struct} equivalent of this type.
     */
    public static Struct GENERIC =
        new StructBuilder().add(byteField1)
                           .add(byteField2)
                           .add(stringField)
                           .add(byteField3)
                           .toStruct();

    @Override
    public boolean isOrderPreserving() { return true; }

    @Override
    public Order getOrder() { return null; }

    @Override
    public boolean isNullable() { return false; }

    @Override
    public boolean isSkippable() { return true; }

    @Override
    public int encodedLength(Pojo2 val) {
      return
          byteField1.encodedLength(val.byteField1Asc) +
          byteField2.encodedLength(val.byteField2Dsc) +
          stringField.encodedLength(val.stringFieldDsc) +
          byteField3.encodedLength(val.byteField3Dsc);
    }

    @Override
    public Class<Pojo2> encodedClass() { return Pojo2.class; }

    @Override
    public void skip(ByteBuffer buff) {
      byteField1.skip(buff);
      byteField2.skip(buff);
      stringField.skip(buff);
      byteField3.skip(buff);
    }

    @Override
    public Pojo2 decode(ByteBuffer buff) {
      return new Pojo2(
        byteField1.decode(buff),
        byteField2.decode(buff),
        stringField.decode(buff),
        byteField3.decode(buff));
    }

    @Override
    public void encode(ByteBuffer buff, Pojo2 val) {
      byteField1.encode(buff, val.byteField1Asc);
      byteField2.encode(buff, val.byteField2Dsc);
      stringField.encode(buff, val.stringFieldDsc);
      byteField3.encode(buff, val.byteField3Dsc);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOrderPreservation() throws Exception {
    Object[] vals = new Object[constructorArgs.length];
    byte[][] encodedGeneric = new byte[constructorArgs.length][];
    byte[][] encodedSpecialized = new byte[constructorArgs.length][];
    Constructor<?> ctor = specialized.encodedClass().getConstructor(Object[].class);
    for (int i = 0; i < vals.length; i++) {
      vals[i] = ctor.newInstance(new Object[] { constructorArgs[i] });
      encodedGeneric[i] = new byte[generic.encodedLength(constructorArgs[i])];
      encodedSpecialized[i] = new byte[specialized.encodedLength(vals[i])];
    }

    // populate our arrays
    for (int i = 0; i < vals.length; i++) {
      generic.encode(ByteBuffer.wrap(encodedGeneric[i]), constructorArgs[i]);
      specialized.encode(ByteBuffer.wrap(encodedSpecialized[i]), vals[i]);
      assertArrayEquals(encodedGeneric[i], encodedSpecialized[i]);
    }

    Arrays.sort(vals);
    Arrays.sort(encodedGeneric, NULL_SAFE_BYTES_COMPARATOR);
    Arrays.sort(encodedSpecialized, NULL_SAFE_BYTES_COMPARATOR);

    for (int i = 0; i < vals.length; i++) {
      assertEquals(
        "Struct encoder does not preserve sort order at position " + i,
        vals[i],
        ctor.newInstance(new Object[] { generic.decode(ByteBuffer.wrap(encodedGeneric[i])) }));
      assertEquals(
        "Specialized encoder does not preserve sort order at position " + i,
        vals[i], specialized.decode(ByteBuffer.wrap(encodedSpecialized[i])));
    }
  }
}
