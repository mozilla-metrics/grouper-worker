package org.mozilla.grouper.hbase;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.mozilla.grouper.base.Config;

/**
 * Stores and reads Grouperfish configuration to/from hadoop config.
 * This way configuration values are transmitted to map/reduce tasks.
 */
public class Util {
    
    private static final String PREFIX = "grouperfish.";
    private static final String PREFIX_MATCHER = "^grouperfish.*";

    /** Stores the given grouperfish conf in the hadoop configuration. */
    public Configuration saveToHadoopConf(Config conf, Configuration hadoopConf) {
        Map<String, String> source = conf.asMap();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            hadoopConf.set(PREFIX + entry.getKey(), entry.getValue());
        }
        return hadoopConf;
    }

    /** Reads Grouperfish configuration from hadoop configuration. Use it in mapred tasks. */
    Config fromHadoopConf(Configuration hadoopConfig) {
        Map<String, String> source = hadoopConfig.getValByRegex(PREFIX_MATCHER);
        Map<String, String> dest = new java.util.HashMap<String, String>();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            dest.put(entry.getKey().substring(PREFIX.length()), entry.getValue());
        }
        return new Config(dest);
    }

    /** Config is required to allow for config-based decisions in utils in the future. */
    public Util(Config conf) { }

}
