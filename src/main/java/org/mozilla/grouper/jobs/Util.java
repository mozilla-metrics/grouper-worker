package org.mozilla.grouper.jobs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mozilla.grouper.base.Assert;
import org.mozilla.grouper.conf.Conf;
import org.mozilla.grouper.jobs.textcluster.TextClusterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Stores and reads Grouperfish configuration to/from hadoop config.
 * This way configuration values are transmitted to map/reduce tasks.
 */
public class Util {

  public
  Util(Conf conf) {
    Assert.nonNull(conf);
    conf_ = conf;
    availableTools_.put(ExportDocuments.NAME,    ExportDocuments.class);
    availableTools_.put(VectorizeDocuments.NAME, VectorizeDocuments.class);
    availableTools_.put(TextClusterTool.NAME,    TextClusterTool.class);
  }


  /** @return The exit status of the job. */
  public
  int run(String toolName, String[] args) {
    Configuration hadoopConf = HBaseConfiguration.create();
    AbstractCollectionTool tool;
    try {
      Class<? extends AbstractCollectionTool> toolType =
        availableTools_.get(toolName);
      tool = toolType.getConstructor(Conf.class, Configuration.class)
      .newInstance(new Object[]{conf_, hadoopConf});
    }
    catch (Exception e) {
      log.error("Could not instantiate the requested tool: " + toolName, e);
      return 1;
    }
    Assert.nonNull(tool);

    // Possibly merge the above with the Cli and handle hadoop config in the
    // abstract tool?
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


  /**
   * Stores the Grouperfish configuration into the hadoop configuration,
   * so that it can be reconstructed from within Map/Reduce tasks.
   */
  void saveConfToHadoopConf(Configuration hadoopConfig) {
    Assert.nonNull(hadoopConfig);
    hadoopConfig.set(HADOOP_CONF_KEY, conf_.toJSON());
  }


  /**
   * Reads Grouperfish configuration from the hadoop configuration.
   * @see #saveConfToHadoopConf(Configuration)
   */
  static
  Conf fromHadoopConf(Configuration hadoopConfig) {
    Assert.nonNull(hadoopConfig);
    final String jsonConf = hadoopConfig.get(HADOOP_CONF_KEY);
    return (new org.mozilla.grouper.conf.Factory()).fromJSON(jsonConf);
  }


  private final
  Map<String, Class<? extends AbstractCollectionTool>> availableTools_ =
    new HashMap<String, Class<? extends AbstractCollectionTool>>();

  private static final Logger log = LoggerFactory.getLogger(Util.class);

  private final Conf conf_;

  private static final String HADOOP_CONF_KEY = "org.mozilla.grouperfish.conf";

}
