package org.mozilla.grouper.hbase;

import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.mozilla.grouper.base.Assert;
import org.mozilla.grouper.base.Config;
import org.mozilla.grouper.model.Cluster;
import org.mozilla.grouper.model.Collection;
import org.mozilla.grouper.model.Document;


public class Factory {

    public Factory(Config conf) {
        conf_ = conf;
        hbaseConf_ = HBaseConfiguration.create();
        if (conf_.hbaseZk() != null) {
            hbaseConf_.set("hbase.zookeeper.quorum", conf_.hbaseZk());
        }
        if (conf_.hbaseZkNode() != null) {
            hbaseConf_.set("zookeeper.znode.parent", conf_.hbaseZkNode());
        }
    }

    public org.apache.hadoop.conf.Configuration hbaseConfig() {
        return hbaseConf_;
    }

    public HTableInterface table(String name) {
        return tableFactory_.createHTableInterface(hbaseConf_,
                                                   Bytes.toBytes(tableName(name)));
    }

    public String tableName(String name) {
        return conf_.prefix() + name;
    }

    public void release(HTableInterface table) {
        tableFactory_.releaseHTableInterface(table);
    }

    /** Row keys that are (hopefully) in sync with those used by the REST service! */
    public Keys keys() {
        return new ReversePartsKeys();
    }

    private static final Map<Class<?>, String> tableByType = new java.util.HashMap<Class<?>, String>();
    static {
        tableByType.put(Cluster.class, Schema.T_CLUSTERS);
        tableByType.put(Document.class, Schema.T_DOCUMENTS);
        tableByType.put(Collection.class, Schema.T_COLLECTIONS);
    }

    public <T> Importer<T> importer(Class<T> model) {
        Assert.check(tableByType.containsKey(model));
        return new Importer<T>(this, tableByType.get(model));
    }

    private final Config conf_;
    private final org.apache.hadoop.conf.Configuration hbaseConf_;
    private final HTableInterfaceFactory tableFactory_ = new HTableFactory();
}
