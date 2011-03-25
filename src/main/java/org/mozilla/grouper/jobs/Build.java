package org.mozilla.grouper.jobs;

import static org.mozilla.grouper.hbase.Schema.T_DOCUMENTS;
import static org.mozilla.grouper.hbase.Schema.CF_CONTENT;
import static org.mozilla.grouper.hbase.Schema.ID;
import static org.mozilla.grouper.hbase.Schema.TEXT;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.mozilla.grouper.base.Assert;
import org.mozilla.grouper.base.Config;
import org.mozilla.grouper.hbase.Factory;
import org.mozilla.grouper.model.CollectionRef;

/** 
 * Export all documents into a directory, one file per map-task.
 * 
 * This is part of the full rebuild and a prerequisite for vectorization.
 */
public class Build extends Configured implements Tool {
    
    final static String NAME = "build";
    public static final String TOOL_USAGE = "Usage: ... build NAMESPACE COLLECTION_KEY";

    static class ExportMapper extends TableMapper<ImmutableBytesWritable, Result> {
        public static enum Counters {
            ROWS_PROCESSED,
            ROWS_USED
        }
        
        @Override
        protected void map(ImmutableBytesWritable key, 
                           Result row, 
                           ExportMapper.Context context) 
        throws java.io.IOException, InterruptedException {
            context.getCounter(Counters.ROWS_PROCESSED).increment(1);
            byte[] documentID = row.getColumnLatest(CF_CONTENT, ID).getValue();
            KeyValue text = row.getColumnLatest(CF_CONTENT, TEXT);
            context.write(new ImmutableBytesWritable(documentID), 
                          new Result(new KeyValue[]{text}));
        };
    }

    private final Config conf_;

    private String outputDir(Config conf, CollectionRef collection, long start) {
        String nsMd5 = DigestUtils.md5Hex(Bytes.toBytes(collection.namespace())).substring(0, 6);
        String ckMd5 = DigestUtils.md5Hex(Bytes.toBytes(collection.key())).substring(0, 6);
        String ts = Long.toString(start);
        return new StringBuilder().append(conf.hadoopPath()).append('/').append(ts).append('/')
               .append(nsMd5).append(ckMd5).toString();
    }

    private Job createSubmittableJob(CollectionRef collection) throws IOException {
        final Configuration hadoopConf = this.getConf();
        new Util(conf_).saveConfToHadoopConf(hadoopConf);
        
        final Factory factory = new Factory(conf_);
        final long start = new Date().getTime();
        final String jobName = "grouper_" + collection.namespace() + "_" + collection.key();
        final String outputDir = outputDir(conf_, collection, start);
        final Job job = new Job(hadoopConf, jobName);
        job.setJarByClass(Build.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Result.class);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        
        // Set optional scan parameters
        Scan scan = new Scan();
        scan.setMaxVersions(1);
        scan.setFilter(new PrefixFilter(Bytes.toBytes(factory.keys().documentPrefix(collection))));
        TableMapReduceUtil.initTableMapperJob(factory.tableName(T_DOCUMENTS), 
                                              scan, ExportMapper.class, null, null, job);
        return job;
    }
   
    private int usage(int status) {
        (status == 0 ? System.out : System.err).println(TOOL_USAGE);
        return status;
    }

    public int run(String[] args) throws Exception {
        if (args.length > 0 && "help".equals(args[0])) return usage(0);
        if (args.length != 2) return usage(1);
        CollectionRef collection = new CollectionRef(args[0], args[1]);
        Job job = createSubmittableJob(collection);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    
    public Build(Config conf, Configuration hadoopConf) {
        Assert.nonNull(conf, hadoopConf);
        this.setConf(hadoopConf);
        conf_ = conf;
    }

}
