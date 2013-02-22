/**
 * This package serializes a wide range of simple and complex key data types
 * into a sort-order preserving byte encoding. Sorting the serialized byte
 * arrays produces the same ordering as the natural sort order of the
 * underlying data type. The package can be used to generate byte-valued
 * serialized keys for sorted key-value data stores such as HBase.
 * 
 * 0. Design
 * 
 * The goal of this project is to produce extremely space efficient byte
 * serializations for common data types while ensuring that the resulting byte
 * array sorts correctly. As a consequence, we provide types optimized for many
 * different situations (32-bit variable length unsigned integers, 64-bit fixed
 * signed integers, etc) so that you do not pay for features that you do not
 * use.
 * 
 * In contrast to more ad-hoc sortable byte serialization designs, we support
 * all values for included types. For example, our double precision floating
 * point encoding supports NaNs, positive/negative zero, subnormals, etc, while
 * our String encoding supports NULLs, the empty string, etc. This is done
 * without compromising space efficiency, usually by taking advantage of the
 * underlying encoding format (i.e. IEEE-745 for doubles, UTF-8 for Strings).
 * Each RowKey class has a JavaDoc precisely describing its serialization
 * format.
 * 
 * RowKeys may be a primitive (single-value) type or complex (composite) type
 * composed of many values. Complex types are themselves composed of other
 * complex types and primitive types.
 * 
 * 1. Supported Primitive Types
 * We support a wide range of primitive (single-value) types:
 * 
 * (i) Variable-Length Integers (Int, IntWritable, Long, LongWritable)
 * Variable-length 32-bit integer, and 64-bit longs encoded in a sort order
 * preserving variable length format similar in design to Zig-Zag encoding.
 * Both signed and unsigned integer types are supported.
 * 
 * Small absolute values such as -1 or 17 take up 1 byte, larger absolute
 * values such as 65536 or 2^28 require more bytes. The maximum length of
 * a variable-length integer is 5 bytes, and a variable-length long is 9 bytes.
 * NULL values are supported without decreasing the range of supported integers
 * or negatively impacting the space efficiency of the encoding.
 * 
 * (ii) Fixed-Width Integers (Int, IntWritable, Long, LongWritable)
 * Fixed-length 32-bit integers and 64-bit longs are serialized directly to the
 * byte array in big endian order. Both signed and unsigned types are supported.
 * This is the only row key type that does not support null values. Useful only
 * when common values are very large (>2^28 for integers, >2^59 for longs),
 * otherwise variable-length integers are much more space efficient.
 * 
 * (iii) Floating Point (Float, FloatWritable, Double, DoubleWritable)
 * Fixed-length 32-bit float and 64-bit double floating point numbers. Null
 * values are supported at no additional space cost by reserving a NaN
 * value unused by Java (it is stripped out by NaN canonicalization during
 * Double.doubleToLongBits). Correctly sorts all values, including subnormals,
 * infinity, positive and negative zero, etc.
 * 
 * (iv) BigDecimal
 * Variable-length bigdecimal format. Scale is encoded as a variable length
 * integer, and the significand is encoded as a variable-length binary coded
 * decimal string. Supports all BigDecimal values, as well as NULL.
 * 
 * (v) String types  (Text, UTF-8 byte array, String)
 * Variable-length format for storing UTF-8 strings. Correctly handles sorting
 * all valid UTF-8 strings, as well as empty string and NULL values. NULLs and
 * string terminators are encoded when necessary by using leveraging invalid
 * UTF-8 header bytes, although in many cases they can be omitted from the
 * serialization entirely (see Section 3).
 * 
 * 2. Complex (Composite) Types
 * Currently, the supported complex type is a struct (record) key, which is
 * used to create a composite key. The struct key is composed a fixed number of
 * field row keys (which may be any valid row key type, including another
 * struct).
 * 
 * For example, let us suppose the user wants a key composed of a timestamp,
 * username, and spam score. The timestamps should be sorted in descending
 * order (so that it is easy to always retrieve the most recent score from the
 * database). For this representation, we could create a struct with three
 * primitive field row keys: a LongWritable with descending sort order, a UTF-8
 * string, and a float.
 * 
 * For convenience, a StructBuilder class is provided to build struct keys more
 * easily, and a StructIterator class is provided to iterate over the fields of
 * a serialized struct.
 * 
 * 3. Important Row Key Methods
 * All row key types are subclasses of type RowKey, and the following methods:
 * (i) get serialized length - Given an object to be serialized, returns the
 * length of the object's serialized representation (so you can allocate
 * storage space)
 * (ii) serialize (writing a type to an immutablebyteswritable or byte array)
 * (iii) deserialize (reading a type from immutablebyteswritable or byte array)
 * (iv) skip (skipping over a serialized type in an immutablebyteswritable
 * without deserialization the object)
 * 
 * 4. Usage Guidelines
 * (1) Prefer Writable or byte types (IntWritable, UTF8, Text) to immutable
 * object (Integer, Long, String) types. The latter cannot be re-used across
 * multiple serialization/deserialization operations. If you have a MapReduce
 * job reading/writing millions or billions of keys, you'll want to use
 * non-immutable types to reduce the number of object instantations. For more
 * information, each RowKey class JavaDoc has a usage section describing its
 * performance characteristics for object instantiation and byte array copying.
 * 
 * (2) Use the most precise format you require...but no more precise
 * For variable-length integers, you will gain slightly more efficient storage
 * by using unsigned integer types instead of signed types (if your integers
 * are unsigned), and using 32-bit integer types instead of 64-bit longs.
 * 
 * However, do not use a 32-bit variable-length integer type unless you are
 * certain that all your values, for the lifetime of your application, will
 * fit in 32-bits. The variable-length long encoding is very efficient, and
 * when compared to the 32-bit integer encoding the cost the additional cost
 * is modest at best (a single bit of overhead for 3/4/5-byte integer
 * encodings, and no overhead for 1 and 2 byte encodings). When in doubt about
 * the range of values you will store, use a signed or unsigned long type.
 * Signed types have 1 additional bit of overhead in comparison to unsigned
 * types (for all integer encoding lengths).
 * 
 * (3) Ascending sort results in slightly smaller encodings for strings,
 * bigdecimals, and any NULL value encoding when mustTerminate is false. This
 * is because when performing ascending sort, we can omit trailing
 * end-of-string or null value bytes and just use the end of the byte array as
 * an implicit terminator as described in the RowKey class's JavaDoc. If you
 * don't have a strong preference on your sort order, ascending sort (in the
 * above situations) results in slightly more efficient encodings. The worst
 * case difference in serialization overhead between ascending and descending
 * order is a single byte per serialized RowKey.
 * 
 * 5. Additional Documentation
 * A set of example classes using the RowKey APIs are provided in src/examples.
 * These classes demonstrate correct API usage, and are a good starting point
 */
package org.apache.hadoop.hbase.util.orderly;