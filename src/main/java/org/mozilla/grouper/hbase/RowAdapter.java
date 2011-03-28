package org.mozilla.grouper.hbase;

import org.apache.hadoop.hbase.client.Put;


interface RowAdapter<S> {
    Put put(S item);
    String key(S item);
}
