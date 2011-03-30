package org.mozilla.grouper.jobs;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mozilla.grouper.base.Assert;
import org.mozilla.grouper.base.Config;
import org.mozilla.grouper.jobs.textcluster.TextClusterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import org.apache.mahout.driver.MahoutDriver;

/**
 * Stores and reads Grouperfish configuration to/from hadoop config.
 * This way configuration values are transmitted to map/reduce tasks.
 */
public class Util {

    private static final Logger log = LoggerFactory.getLogger(Util.class);

    private final Config conf_;

    private static final String PREFIX = "grouperfish.";
    private static final String PREFIX_MATCHER = "^grouperfish.*";

    /** Stores the given grouperfish conf in the hadoop configuration. */
    public Configuration saveConfToHadoopConf(Configuration hadoopConf) {
        Assert.nonNull(hadoopConf);
        Map<String, String> source = conf_.asMap();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            save(hadoopConf, entry.getKey(), entry.getValue());
        }
        return hadoopConf;
    }

    /** Reads Grouperfish configuration from hadoop configuration. Use it in mapred tasks. */
    static Config fromHadoopConf(Configuration hadoopConfig) {
        Map<String, String> source = hadoopConfig.getValByRegex(PREFIX_MATCHER);
        Map<String, String> dest = new java.util.HashMap<String, String>();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            dest.put(entry.getKey().substring(PREFIX.length()), entry.getValue());
        }
        return new Config(dest);
    }

    static void save(Configuration hadoopConfig, String key, String value) {
        hadoopConfig.set(PREFIX + key, value);
    }

    static String load(Configuration hadoopConfig, String key) {
        return hadoopConfig.get(PREFIX + key);
    }

    /** Config is required to allow for config-based decisions in utils in the future. */
    public Util(Config conf) {
        Assert.nonNull(conf);
        conf_ = conf;
        availableTools_.put(ExportDocuments.NAME,    ExportDocuments.class);
        availableTools_.put(VectorizeDocuments.NAME, VectorizeDocuments.class);
        availableTools_.put(TextClusterTool.NAME,    TextClusterTool.class);
    }

    // TODO:
    // - Either use properties to register jobs like Mahout, or use spring...
    private final Map<String, Class<? extends AbstractCollectionTool>> availableTools_ =
        new java.util.HashMap<String, Class<? extends AbstractCollectionTool>>();


    /** @return the exit status of the job */
    public int run(String toolName, String[] args) {
        Configuration hadoopConf = HBaseConfiguration.create();
        AbstractCollectionTool tool;
        try {
            Class<? extends AbstractCollectionTool> toolType = availableTools_.get(toolName);
            tool = toolType.getConstructor(Config.class, Configuration.class)
                           .newInstance(new Object[]{conf_, hadoopConf});
        }
        catch (Exception e) {
            log.error("Could not instantiate the requested tool: " + toolName, e);
            return 1;
        }
        Assert.nonNull(tool);

        // Possibly merge the above with the Cli and handle hadoop config in the abstract tool?
        String[] otherArgs;
        try {
            otherArgs = new GenericOptionsParser(hadoopConf, args).getRemainingArgs();
        } catch (IOException e) {
            log.error("Error running job: " + toolName, e);
            return 1;
        }
        try {
            return tool.run(otherArgs);
        }
        catch (Exception e) {
            log.error("Error running job: " + toolName, e);
            return 1;
        }
    }
}
