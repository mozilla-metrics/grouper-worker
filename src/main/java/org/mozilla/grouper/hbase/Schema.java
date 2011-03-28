package org.mozilla.grouper.hbase;

import org.apache.hadoop.hbase.util.Bytes;


/** While the schema is managed by the REST frontend, these help not to suffer from typos. */
public class Schema {

    // Table Suffixes
    public static final String T_DOCUMENTS = "documents";
    public static final String T_COLLECTIONS = "collections";
    public static final String T_CLUSTERS = "clusters";

    // Column Families
    public static final byte[] CF_CONTENT = Bytes.toBytes("content");
    public static final byte[] CF_DOCUMENTS = Bytes.toBytes("documents");
    public static final byte[] CF_CONFIGURATIONS = Bytes.toBytes("configurations");
    public static final byte[] CF_MAHOUT = Bytes.toBytes("mahout");
    public static final byte[] CF_META = Bytes.toBytes("meta");

    // Column Qualifiers
    public static final byte[] NAMESPACE = Bytes.toBytes("namespace");
    public static final byte[] LAST_REBUILD = Bytes.toBytes("rebuild");
    public static final byte[] COLLECTION_KEY = Bytes.toBytes("collectionKey");
    public static final byte[] ID = Bytes.toBytes("id");
    public static final byte[] TEXT = Bytes.toBytes("text");
    public static final byte[] LABEL = Bytes.toBytes("label");
    public static final byte[] SIZE = Bytes.toBytes("size");

}
