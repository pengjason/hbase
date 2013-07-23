
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.types.DataType;
import org.apache.hadoop.hbase.types.LegacyBytesFixedLength;
import org.apache.hadoop.hbase.types.Struct;

/**
 * A rough approximation of Kiji's FormattedEntityId, using the HDataType API.
 */
@SuppressWarnings("rawtypes")
public class KijiFormattedEntityId extends Struct {

  /**
   * Implements (roughly) the hash portion of <code>FormattedEntityId#makeHbaseRowKey</code>.
   */
  public static class KijiHash extends LegacyBytesFixedLength {

    public KijiHash(int length) { super(length); }

    @Override
    public void encode(ByteBuffer buff, byte[] val) {
      encode(buff, val, 0, val.length);
    }

    @Override
    public void encode(ByteBuffer buff, byte[] val, int offset, int length) {
      try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(val, offset, length);
        byte[] digest = md.digest();
        super.encode(buff, digest, 0, length);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("Error computing MD5 hash", e);
      }
    }
  }

  protected final int rangeScanStartIndex;

  public KijiFormattedEntityId(int hashSize, int rangeScanStartIndex, DataType[] components) {
    super((DataType[]) ArrayUtils.addAll(new DataType[] { new KijiHash(hashSize) }, components));
    this.rangeScanStartIndex = rangeScanStartIndex;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void encode(ByteBuffer buff, Object[] val) {
    // set aside n bytes for salt
    int start = buff.position(),
        componentsStart = buff.position() + ((KijiHash) fields[0]).encodedLength(null),
        componentsEnd = 0,
        componentsLength = 0,
        rangeEnd = 0;
    buff.position(componentsStart);

    // write out our components, taking note of the final component used in hash
    for (int i = 1; i < fields.length; i++) {
      fields[i].encode(buff, val[i]);
      if (i == rangeScanStartIndex) {
        rangeEnd = buff.position();
      }
    }
    assert 0 != rangeEnd;

    // take note of how far we've come
    componentsEnd = buff.position();
    componentsLength = componentsEnd - componentsStart;

    // now write the hashed value
    buff.position(start);
    ((KijiHash) fields[0]).encode(buff, buff.array(), componentsStart, componentsLength);

    // and restore position
    buff.position(componentsEnd);
  }
}
