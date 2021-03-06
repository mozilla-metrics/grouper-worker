package org.mozilla.grouper.jobs;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.mozilla.grouper.conf.Conf;
import org.mozilla.grouper.hbase.Factory;
import org.mozilla.grouper.model.Collection;
import org.mozilla.grouper.model.Collection.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Fully rebuilds all collection that are out of date.
 */
public class RebuildAll extends Configured implements Tool {

  public RebuildAll(Conf conf, Configuration hadoopConf) {
    conf_ = conf;
    super.setConf(hadoopConf);
  }

  @Override public int
  run(final String[] conf) throws Exception {
    CollectionTool rebuild = new Rebuild(conf_, getConf());
    final List<Collection> todo = new ArrayList<Collection>();
    for (Collection c : new Factory(conf_).source(Collection.class)) {
      final Long rebuilt = c.get(Attribute.REBUILT);
      final Long modified = c.get(Attribute.MODIFIED);
      // TODO:
      // Push these down to the HBase scannners, so number of collections can
      // grow beyond mere thousands.
      if (modified == null) continue;
      if (rebuilt != null && rebuilt > modified) continue;
      todo.add(c);
    }


    for (Collection c : todo) {
      final long timestamp = new Date().getTime();
      log.info("Rebuilding collection {} at {}", c.ref().key(), timestamp);
      rebuild.run(c.ref(), timestamp);
    }

    return 0;
  }


  static String NAME = "rebuild_all";

  private final Conf conf_;

  private static final Logger log = LoggerFactory.getLogger(RebuildAll.class);

}
