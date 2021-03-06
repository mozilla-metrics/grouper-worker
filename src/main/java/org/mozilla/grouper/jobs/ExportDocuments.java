package org.mozilla.grouper.jobs;

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
import org.mozilla.grouper.conf.Conf;
import org.mozilla.grouper.hbase.Factory;
import org.mozilla.grouper.hbase.Schema.Documents;
import org.mozilla.grouper.model.CollectionRef;
import org.mozilla.grouper.model.Document;


/**
 * Export all documents into a directory, one file per map-task.
 *
 * This is part of the full rebuild and a prerequisite for vectorization.
 *
 * TODO: We want a better partitioner so that regions are only looked at by a
 *       mapper if they overlap with our prefix.
 */
public class ExportDocuments extends AbstractCollectionTool {

  final static String NAME = "export_documents";


  static class ExportMapper extends TableMapper<Text, Text> {
    public static enum Counters {
      ROWS_USED
    }

    @Override protected
    void map(ImmutableBytesWritable key,
             Result row,
             ExportMapper.Context context)
    throws java.io.IOException, InterruptedException {
      context.getCounter(Counters.ROWS_USED).increment(1);
      byte[] documentID =
        row.getColumnLatest(Documents.Main.FAMILY,
                            Documents.Main.ID.qualifier).getValue();
      KeyValue text = row.getColumnLatest(Documents.Main.FAMILY,
                                          Documents.Main.TEXT.qualifier);
      context.write(new Text(documentID), new Text(text.getValue()));
    };
  }


  public
  ExportDocuments(Conf conf, Configuration hadoopConf) {
    super(conf, hadoopConf);
  }


  @Override protected
  Job createSubmittableJob(CollectionRef collection, long timestamp)
  throws Exception {
    final Configuration hadoopConf = this.getConf();
    new Util(conf_).saveConfToHadoopConf(hadoopConf);

    final Path outputDir = util_.outputDir(collection, timestamp, this);
    final String jobName = jobName(collection, timestamp);
    final Job job = new Job(hadoopConf, jobName);

    job.setJarByClass(AbstractCollectionTool.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job, outputDir);

    // Set optional scan parameters
    final Scan scan = new Scan();
    scan.setMaxVersions(1);
    final Factory factory = new Factory(conf_);
    final String prefix = factory.keys().documentPrefix(collection);
    scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));

    TableMapReduceUtil.initTableMapperJob(factory.tableName(Document.class),
                                          scan, ExportMapper.class, null, null,
                                          job);
    return job;
  }


  @Override public
  String name() {
    return NAME;
  }

}
