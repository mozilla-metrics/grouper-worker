package org.mozilla.grouper.hbase;

import org.apache.hadoop.hbase.client.Put;

/** Handles HBase CRUD for individual model objects. */
interface RowAdapter<S> {
    Put put(S item);
    String key(S item);
}
