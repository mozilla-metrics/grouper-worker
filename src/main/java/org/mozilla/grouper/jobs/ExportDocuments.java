package org.mozilla.grouper.jobs;

import static org.mozilla.grouper.hbase.Schema.CF_CONTENT;
import static org.mozilla.grouper.hbase.Schema.ID;
import static org.mozilla.grouper.hbase.Schema.TEXT;
import static org.mozilla.grouper.hbase.Schema.T_DOCUMENTS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.mozilla.grouper.base.Config;
import org.mozilla.grouper.hbase.Factory;
import org.mozilla.grouper.model.CollectionRef;

/**
 * Export all documents into a directory, one file per map-task.
 *
 * This is part of the full rebuild and a prerequisite for vectorization.
 *
 * :TODO: We want a better partitioner (or something) here, so that regions
 * are only scanned if they can contain values with our prefix.
 */
public class ExportDocuments extends AbstractCollectionTool {

    final static String NAME = "export_documents";
    public static final String TOOL_USAGE = "%s NAMESPACE COLLECTION_KEY\n";

    static class ExportMapper extends TableMapper<Text, Text> {
        public static enum Counters {
            ROWS_USED
        }

        @Override
        protected void map(ImmutableBytesWritable key,
                           Result row,
                           ExportMapper.Context context)
        throws java.io.IOException, InterruptedException {
            context.getCounter(Counters.ROWS_USED).increment(1);
            byte[] documentID = row.getColumnLatest(CF_CONTENT, ID).getValue();
            KeyValue text = row.getColumnLatest(CF_CONTENT, TEXT);
            context.write(new Text(documentID), new Text(text.getValue()));
        };
    }

    public ExportDocuments(Config conf, Configuration hadoopConf) { super(conf, hadoopConf); }

    @Override
    protected Job createSubmittableJob(CollectionRef collection, long timestamp) throws Exception {
        final Configuration hadoopConf = this.getConf();
        new Util(conf_).saveConfToHadoopConf(hadoopConf);

        final Path outputDir = outputDir(collection, timestamp);
        final String jobName = jobName(collection, timestamp);
        final Job job = new Job(hadoopConf, jobName);
        job.setJarByClass(AbstractCollectionTool.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, outputDir);

        // Set optional scan parameters
        Scan scan = new Scan();
        scan.setMaxVersions(1);
        final Factory factory = new Factory(conf_);
        scan.setFilter(new PrefixFilter(Bytes.toBytes(factory.keys().documentPrefix(collection))));
        TableMapReduceUtil.initTableMapperJob(factory.tableName(T_DOCUMENTS),
                                              scan, ExportMapper.class, null, null, job);
        return job;
    }

    @Override
    protected String name() { return NAME; }

}
