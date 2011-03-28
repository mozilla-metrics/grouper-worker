package org.mozilla.grouper.jobs;

import java.util.Date;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.mortbay.log.Log;
import org.mozilla.grouper.base.Assert;
import org.mozilla.grouper.base.Config;
import org.mozilla.grouper.model.CollectionRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A hadoop tool that operates on a collection of documents.
 * Collection tools should be stateless (except for having configuration).
 */
public abstract class AbstractCollectionTool extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(AbstractCollectionTool.class);

    protected final Config conf_;

    /** The name of the tool (not the job name). */
    protected abstract String name();

    /**
     * Run tool (on hadoop).
     *
     * @param collection The collection this job is about.
     * @param timestamp  What is considered "the time" of the job, e.g. for
     *                   a cluster rebuild, this becomes the "last rebuild
     *                   time".
     * @return A job that can be submitted.
     */

    protected int run(CollectionRef collection, long timestamp) throws Exception {
        Job job = createSubmittableJob(collection, timestamp);
        Log.info("Running job: {}", job.getJobName());
        return job.waitForCompletion(true) ? 0 : 1;
    }

    protected String jobName(CollectionRef c, long timestamp) {
        return String.format("Grouperfish:%s %s/%s/%s", name(), timestamp, c.namespace(), c.key());
    }

    /**
     * Create a job to run on hadoop.
     * Tools can implement this to create their job, or override run.
     */
    protected Job createSubmittableJob(CollectionRef collection, long timestamp) throws Exception {
        return null;
    }

    /** Possibly tool-specific usage info. %s is replaced with the tool name. */
    protected String synopsis() {
        return "%s NAMESPACE COLLECTION_KEY\n";
    }

    protected int usage(int status) {
        (status == 0 ? System.out : System.err).format(synopsis(), name());
        return status;
    }

    public AbstractCollectionTool(Config conf, Configuration hadoopConf) {
        Assert.nonNull(conf, hadoopConf);
        this.setConf(hadoopConf);
        conf_ = conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length > 0 && "help".equals(args[0])) return usage(0);
        long timestamp = new Date().getTime();
        if (args.length == 4 && "--timestamp".equals(args[2])) timestamp = Long.valueOf(args[3]);
        else if (args.length > 3) return usage(1);
        CollectionRef collection = new CollectionRef(args[0], args[1]);

        final Path dest = outputDir(collection, timestamp);
        FileSystem fs = FileSystem.get(dest.toUri(), getConf());
        if (fs.exists(dest)) {
            // Should not happen in an actual installation, due to locking + timestamp.
            // Can very well happen during development.
            log.warn("Output dir {} already exists. This should not happen. Pruning.", dest);
            fs.delete(dest, true);
        }

        return run(collection, timestamp);
    }

    public Path outputDir(CollectionRef collection, long timestamp) {
        String nsMd5 = DigestUtils.md5Hex(Bytes.toBytes(collection.namespace())).substring(0, 6);
        String ckMd5 = DigestUtils.md5Hex(Bytes.toBytes(collection.key())).substring(0, 6);
        String ts = Long.toString(timestamp);
        return new Path(new StringBuilder().append(conf_.dfsBase()).append('/')
                                           .append(ts).append('/')
                                           .append(nsMd5).append('_').append(ckMd5).append('/')
                                           .append(name()).toString());
    }
}
