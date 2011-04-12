package org.mozilla.grouper.hbase;

import org.apache.hadoop.hbase.util.Bytes;


/**
 * Helps to address table names, column families and column qualifiers without
 * duplicating their string / byte[] literals too much (DRY pinciple).
 *
 * This <em>static</em> collection of identifiers gives some compile time
 * safety when referencing database items. It is not to be confused with the
 * model classes.
 */
public abstract class Schema {

  public static
  interface Documents {

    public static final String TABLE = table(Documents.class);

    public static
    enum Main {
      NAMESPACE,
      COLLECTION_KEY,
      ID,
      TEXT,
      MEMBER_OF;

      public byte[] qualifier = qualifier(this);
      public static final byte[] FAMILY = family(Main.class);
    }

    public static
    enum Processing {
      ID,
      VECTOR_IDF,
      VECTOR_TFIDF;

      public byte[] qualifier = qualifier(this);
      public static final byte[] FAMILY = family(Processing.class);
    }
  }


  public static
  interface Collections {

    public static final String TABLE = table(Collections.class);

    public static
    enum Main {
      NAMESPACE,
      KEY,
      ID,
      SIZE,
      CONFIGURATION;

      public byte[] qualifier = qualifier(this);
      public static final byte[] FAMILY = family(Main.class);
    }

    public static
    enum Processing {
      DICTIONARY,
      ID;

      public byte[] qualifier = qualifier(this);
      public static final byte[] FAMILY = family(Processing.class);
    }
  }


  public static
  interface Clusters {

    public static final String TABLE = table(Clusters.class);

    public static
    enum Main {
      NAMESPACE,
      KEY,
      TIMESTAMP,
      LABEL,
      SIZE;

      public byte[] qualifier = qualifier(this);
      public static final byte[] FAMILY = family(Main.class);
    }

    public static
    enum Documents {
      ; // The document IDs of the cluster members are used.
      public static final byte[] FAMILY = family(Documents.class);
    }

  }


  /**
   * Produces a multipart column qualifier starting with the given qualifier
   * prefix. Parts are separated using colon ':'. Example: define multiple
   * configurations by using the {@link Collections.Main#CONFIGURATION}
   * qualifier as the prefix, a configuration name (e.g. "DEFAULT") as infix,
   * and individual configuration keys as suffix.
   *
   * @param column First part of the multipart qualifier.
   * @param parts The following parts.
   * @return A column qualifier for use with the HBase api.
   */
  public static
  byte[] qualifier(Enum<?> column, String... parts) {
    StringBuilder builder = new StringBuilder(column.name().toLowerCase());
    for (String part : parts) builder.append(':').append(part);
    return Bytes.toBytes(builder.toString());
  }


  private static
  byte[] family(Class<?> clazz) {
    return Bytes.toBytes(clazz.getSimpleName().toLowerCase());
  }


  private static
  byte[] qualifier(Enum<?> column) {
    return Bytes.toBytes(column.name().toLowerCase());
  }


  private static
  String table(Class<?> clazz) {
    return clazz.getSimpleName().toLowerCase();
  }

}
