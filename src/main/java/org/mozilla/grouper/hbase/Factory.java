package org.mozilla.grouper.hbase;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.mozilla.grouper.base.Assert;
import org.mozilla.grouper.conf.Conf;
import org.mozilla.grouper.hbase.keys.Keys;
import org.mozilla.grouper.hbase.keys.ReversePartsKeys;
import org.mozilla.grouper.hbase.keys.SimpleKeys;
import org.mozilla.grouper.model.Cluster;
import org.mozilla.grouper.model.Collection;
import org.mozilla.grouper.model.Document;
import org.mozilla.grouper.model.Model;


/**
 * Provides database services.
 *
 * TODO: Actually this should be split into two interfaces:
 * 1) Public Storage api hiding HBase from clients by giving them Importer etc.
 * 2) The other as an internal api to services like keys, schema and so on.
 */
public class Factory {

  public
  Factory(Conf conf) {
    conf_ = conf;

    prefix_ = conf_.get(CONF_PREFIX);
    Assert.nonNull(prefix_);

    final String keyScheme = conf_.get(CONF_KEYS_SCHEME);
    Assert.nonNull(keyScheme);
    if ("REVERSE_PARTS".equals(keyScheme)) keys_ = new ReversePartsKeys();
    else if ("SIMPLE".equals(keyScheme)) keys_ = new SimpleKeys();
    else keys_ = Assert.unreachable(Keys.class,
                                    "Unknown key scheme: %s", keyScheme);

    hbaseConf_ = HBaseConfiguration.create();
    final String zkQuorum = conf_.get(CONF_ZK_QUORUM);
    if (zkQuorum != null) hbaseConf_.set("hbase.zookeeper.quorum", zkQuorum);
    final String zkZnode = conf_.get(CONF_ZK_ZNODE);
    if (zkZnode != null) hbaseConf_.set("zookeeper.znode.parent", zkZnode);
  }



  public <T extends Model>
  Importer<T> importer(Class<T> model) {
    Assert.check(tableByType_.containsKey(model));
    return new Importer<T>(this, model);
  }


  /** Row keys that must be in sync with those used by the REST service. */
  public
  Keys keys() {
    return keys_;
  }


  public
  String tableName(final Class<?> model) {
    return prefix_ + tableByType_.get(model);
  }


  public
  Configuration hbaseConfig() {
    return hbaseConf_;
  }


  HTableInterface table(final Class<?> model) {
    byte[] tableName = Bytes.toBytes(tableName(model));
    return tableFactory_.createHTableInterface(hbaseConf_, tableName);
  }


  void release(HTableInterface table) {
    tableFactory_.releaseHTableInterface(table);
  }


  private static final Map<Class<?>, String> tableByType_;
  static {
    tableByType_ = new HashMap<Class<?>, String>();
    tableByType_.put(Cluster.class, Schema.Clusters.TABLE);
    tableByType_.put(Document.class, Schema.Documents.TABLE);
    tableByType_.put(Collection.class, Schema.Collections.TABLE);
  }


  private static final String CONF_ZK_QUORUM = "storage:hbase:zookeeper:quorum";
  private static final String CONF_ZK_ZNODE = "storage:hbase:zookeeper:znode";
  private static final String CONF_PREFIX = "general:prefix";
  private static final String CONF_KEYS_SCHEME = "storage:hbase:keys:scheme";

  private final Keys keys_;
  private final Conf conf_;
  private final String prefix_;
  private final Configuration hbaseConf_;
  private final HTableInterfaceFactory tableFactory_ = new HTableFactory();

}
