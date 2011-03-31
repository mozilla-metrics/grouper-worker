package org.mozilla.grouper.hbase;

import java.util.HashMap;
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
import org.mozilla.grouper.model.Model;

/**
 * Provides database services.
 *
 * TODO: Actually this should be split into two interfaces. 1) a storage API hiding HBase details
 * from clients by giving them Importer etc. The other as an internal interface to services like
 * keys, schema and so on.
 */
public class Factory {

    public String tableName(Class<?> model) {
        return conf_.prefix() + tableByType.get(model);
    }

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

    HTableInterface table(Class<?> model) {
        return tableFactory_.createHTableInterface(hbaseConf_,
                                                   Bytes.toBytes(tableName(model)));
    }

    void release(HTableInterface table) {
        tableFactory_.releaseHTableInterface(table);
    }

    /** Row keys that are (hopefully) in sync with those used by the REST service! */
    public Keys keys() {
        return new ReversePartsKeys();
    }

    private static final Map<Class<?>, String> tableByType = new HashMap<Class<?>, String>();
    static {
        tableByType.put(Cluster.class, Schema.T_CLUSTERS);
        tableByType.put(Document.class, Schema.T_DOCUMENTS);
        tableByType.put(Collection.class, Schema.T_COLLECTIONS);
    }

    public <T extends Model> Importer<T> importer(Class<T> model) {
        Assert.check(tableByType.containsKey(model));
        return new Importer<T>(this, model);
    }

    private final Config conf_;
    private final org.apache.hadoop.conf.Configuration hbaseConf_;
    private final HTableInterfaceFactory tableFactory_ = new HTableFactory();
}
