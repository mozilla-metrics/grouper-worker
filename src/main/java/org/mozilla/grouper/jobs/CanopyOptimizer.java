package org.mozilla.grouper.jobs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.mozilla.grouper.base.Assert;
import org.mozilla.grouper.base.Config;
import org.mozilla.grouper.model.CollectionRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Finds good t1 and t2 parameters for canopy clustering by calculating a histogram over
 * vector distances.
 */
public class CanopyOptimizer extends AbstractCollectionTool {

    final static String NAME = "histogram";

    public static final String TOOL_USAGE = "%s NAMESPACE COLLECTION_KEY\n";

    private static final Logger log = LoggerFactory.getLogger(CanopyOptimizer.class);

    @Override
    protected String name() { return NAME; }

    public CanopyOptimizer(Config conf, Configuration hadoopConf) { super(conf, hadoopConf); }

    @Override
    protected Job createSubmittableJob(CollectionRef collection, long timestamp) throws Exception {
        final Configuration hadoopConf = getConf();

        AbstractCollectionTool source = new VectorizeDocuments(conf_, hadoopConf);
        final Path inputDir = new Path(source.outputDir(collection, timestamp), "tf-vectors");
        final Path outputDir = outputDir(collection, timestamp);
        final String jobName = jobName(collection, timestamp);

        final Job job = new Job(hadoopConf, jobName);
        job.setJarByClass(DeltaHistogram.class);
        job.setMapperClass(RandomSampler.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);
        SequenceFileInputFormat.setInputPaths(job, inputDir);

        job.setReducerClass(DeltaHistogram.class);
        job.setNumReduceTasks(1);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(HistogramWritable.class);
        FileOutputFormat.setOutputPath(job, outputDir);
        return job;
    }

    @Override
    protected int run(CollectionRef collection, long timestamp) throws Exception {
        new Util(conf_).saveConfToHadoopConf(getConf());
        Histogram logHistogram = runIteration(collection, timestamp, 0, 0);
        double nextStep = logHistogram.category(logHistogram.maxIndex());

        /*Histogram linHistogram = */runIteration(collection, timestamp, nextStep, 0);
        return 0;
    }

    private Histogram runIteration(CollectionRef collection,
                                   long timestamp,
                                   double step,
                                   double offset) throws Exception {

        Util.save(getConf(), DeltaHistogram.STEP, Double.valueOf(step).toString());
        Util.save(getConf(), DeltaHistogram.OFFSET, Double.valueOf(offset).toString());

        // Run the sampler + histogram builder job
        log.info(String.format("Creating a histogram (offset = %4.4f, step-size = %4.4f)",
                               offset, step));
        super.run(collection, timestamp);
        Path p = new Path(outputDir(collection, timestamp), "part-r-00000");
        Histogram histogram = fromSequenceFile(p);
        log.info(String.format("Histogram: %s", histogram));

        int maxIndex = histogram.maxIndex();
        double category = histogram.category(maxIndex);
        log.info(String.format("Most common category: d>=%s (portion: %s)",
                               category, histogram.portion(maxIndex)));
        p.getFileSystem(getConf()).delete(outputDir(collection, timestamp), true);

        return histogram;
    }

    static class RandomSampler extends Mapper<Text, VectorWritable, IntWritable, VectorWritable> {
        final Random rand_ = new Random();
        final double p_ = 0.05;
        final int R = (int) Math.round(Math.pow(10, 6));

        @Override
        protected void map(Text _, VectorWritable v, Context context)
        throws IOException, InterruptedException {
            // pick only about a portion p of the input vectors.
            if (rand_.nextDouble() >= p_) return;
            // ...and randomize their order
            IntWritable r = new IntWritable();
            r.set(rand_.nextInt(R));
            context.write(r, v);
        }
    }

    static class DeltaHistogram extends Reducer<IntWritable, VectorWritable, Text, HistogramWritable> {

        /**
         * A double is saved here for the multiplier of the linear scale to use.
         * zero/unset means: use log scale.
         */
        public static final String STEP = "DeltaHistogram.STEP";
        public static final String OFFSET = "DeltaHistogram.OFFSET";

        public static enum Counters { TOO_SMALL, WITHIN, TOO_LARGE }

        final DistanceMeasure dm_ = new ManhattanDistanceMeasure();
        Histogram histogram_;
        Vector last_ = null;

        @Override
        protected void setup(DeltaHistogram.Context context)
        throws IOException ,InterruptedException {
            final Double stepSize = Double.valueOf(Util.load(context.getConfiguration(), STEP));
            if (stepSize.equals(0.0)) {
                histogram_ = new Histogram();
                return;
            }
            Double offset = Double.valueOf(Util.load(context.getConfiguration(), OFFSET));
            histogram_ = new Histogram(offset, stepSize);
        };

        @Override
        protected void reduce(IntWritable key,
                              java.lang.Iterable<VectorWritable> valuesIterable,
                              DeltaHistogram.Context context)
        throws IOException, InterruptedException {
            Iterator<VectorWritable> values = valuesIterable.iterator();
            if (last_ == null) last_ = values.next().get();
            while (values.hasNext()) {
                Vector next = values.next().get();
                double d = dm_.distance(next, last_);
                histogram_.add(d);
                if (d < histogram_.left()) context.getCounter(Counters.TOO_SMALL).increment(1);
                if (d >= histogram_.right()) context.getCounter(Counters.TOO_LARGE).increment(1);
                context.getCounter(Counters.WITHIN).increment(1);
                last_ = next;
            }
        }

        @Override
        protected void cleanup(DeltaHistogram.Context context)
        throws IOException ,InterruptedException {
            context.write(new Text(dm_.getClass().getSimpleName()),
                          new HistogramWritable(histogram_));
        };
    }

    static class HistogramWritable implements Writable {

        Histogram value_;

        public HistogramWritable() { }
        public HistogramWritable(Histogram value) { value_ = value; }
        public Histogram get() { return value_; }

        @Override
        public void write(DataOutput out) throws IOException {
            Assert.nonNull(value_);
            int k = value_.k();
            out.writeInt(k);
            for (int i = 0; i < k; ++i) out.writeDouble(value_.category(i));
            for (int i = 0; i < k; ++i) out.writeInt(value_.count(i));
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int k = in.readInt();
            double[] categories = new double[k];
            for (int i = 0; i < k; ++i) categories[i] = in.readDouble();
            int[] counts = new int[k];
            for (int i = 0; i < k; ++i) counts[i] = in.readInt();
            value_ = new Histogram(categories, counts);
        }

        public static HistogramWritable read(DataInput in) throws IOException {
            HistogramWritable w = new HistogramWritable();
            w.readFields(in);
            return w;
        }

    }

    Histogram fromSequenceFile(Path p)
    throws IOException {
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(p.getFileSystem(getConf()), p, getConf());
            final Text key =
                (Text) ReflectionUtils.newInstance(reader.getKeyClass(), getConf());
            final HistogramWritable histogram =
                (HistogramWritable) ReflectionUtils.newInstance(reader.getValueClass(), getConf());
            reader.next(key, histogram);
            return histogram.get();
        }
        finally {
            IOUtils.closeStream(reader);
        }
    }


}
