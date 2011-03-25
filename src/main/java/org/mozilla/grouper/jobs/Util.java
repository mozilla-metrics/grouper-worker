package org.mozilla.grouper.jobs;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.mozilla.grouper.base.Assert;
import org.mozilla.grouper.base.Config;

// import org.apache.mahout.driver.MahoutDriver;

/**
 * Stores and reads Grouperfish configuration to/from hadoop config.
 * This way configuration values are transmitted to map/reduce tasks.
 */
public class Util {

    private final Config conf_;

    private static final String PREFIX = "grouperfish.";
    private static final String PREFIX_MATCHER = "^grouperfish.*";

    /** Stores the given grouperfish conf in the hadoop configuration. */
    public Configuration saveConfToHadoopConf(Configuration hadoopConf) {
        Assert.nonNull(hadoopConf);
        Map<String, String> source = conf_.asMap();
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
    public Util(Config conf) {
        Assert.nonNull(conf);
        conf_ = conf;
    }

    /** @return the exit status of the job */
    public int run(String toolName, String[] args) {
        Configuration hadoopConf = HBaseConfiguration.create();
        Tool tool = null;
        if (ExportDocuments.NAME.equals(toolName)) {
            tool = new ExportDocuments(conf_, hadoopConf);
        }
        else if (VectorizeDocuments.NAME.equals(toolName)) {
            tool = new VectorizeDocuments(conf_, hadoopConf);
        }
        Assert.nonNull(tool);

        String[] otherArgs;
        try {
            otherArgs = new GenericOptionsParser(hadoopConf, args).getRemainingArgs();
        } catch (IOException e) {
            System.err.format("Error parsing hadoop/hbase options for job: %s!\n", tool);
            e.printStackTrace();
            return 1;
        }
        try {
            return tool.run(otherArgs);
        }
        catch (Exception e) {
            System.err.format("Error running job: %s!\n", tool);
            e.printStackTrace();
            return 1;
        }
    }
}
