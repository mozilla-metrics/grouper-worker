package org.mozilla.grouper.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.mozilla.grouper.model.Model;


/** Handles HBase CRUD for individual model objects. */
interface RowAdapter<S extends Model> {
  Put put(S item);
  String key(S item);
  S read(Result next);
}
