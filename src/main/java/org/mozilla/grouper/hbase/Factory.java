package org.mozilla.grouper.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.mozilla.grouper.base.Configuration;


public class Factory {

    public Factory(Configuration conf) {
        conf_ = conf;
        hbaseConf_ = HBaseConfiguration.create();
        // hbaseConf_.set("hbase.zookeeper.quorum", conf_.hbase());
    }
    
    public HTableInterface table(String name) {
        return tableFactory_.createHTableInterface(hbaseConf_,
                                                   Bytes.toBytes(conf_.tableName(name)));
    }
    
    public void release(HTableInterface table) {
        tableFactory_.releaseHTableInterface(table);
    }
    
    /** Row keys that are (hopefully) in sync with those used by the REST service! */
    public Keys keys() {
        return new SimpleKeys();
    }
    
    private final Configuration conf_;
    private final org.apache.hadoop.conf.Configuration hbaseConf_;    
    private final HTableInterfaceFactory tableFactory_ = new HTableFactory();
}
