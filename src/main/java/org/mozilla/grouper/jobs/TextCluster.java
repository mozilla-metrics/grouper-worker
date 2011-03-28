package org.mozilla.grouper.jobs;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.mozilla.grouper.base.Assert;
import org.mozilla.grouper.base.Config;
import org.mozilla.grouper.jobs.textcluster.Cluster;
import org.mozilla.grouper.jobs.textcluster.IndexClusterer;
import org.mozilla.grouper.model.CollectionRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextCluster extends AbstractCollectionTool {

    public TextCluster(Config conf, Configuration hadoopConf) {
        super(conf, hadoopConf);
    }

    private static final Logger log = LoggerFactory.getLogger(IndexClusterer.class);
    public static final String NAME = "textcluster";

    @Override
    protected String name() { return NAME; }

    @Override
    protected int run(CollectionRef collection, long timestamp)
    throws Exception {
        return run(collection, timestamp, true);
    }


    @Override
    protected Job createSubmittableJob(CollectionRef collection, long timestamp) throws Exception {
        Assert.unreachable("Implement me!!!");
        return null;
    }

    protected int run(CollectionRef collection, long timestamp, boolean sequential)
    throws Exception {
        if (!sequential) {
            return super.run(collection, timestamp);
        }

        // In memory version.

        final AbstractCollectionTool source = new VectorizeDocuments(conf_, getConf());
        final Path inputDir = source.outputDir(collection, timestamp);
        final Path p = new Path(inputDir, "tfidf-vectors/part-r-00000");

        // Read vectors...
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(p.getFileSystem(getConf()), p, getConf());

            final Text key =
                (Text) ReflectionUtils.newInstance(reader.getKeyClass(), getConf());

            final VectorWritable vector =
                (VectorWritable) ReflectionUtils.newInstance(reader.getValueClass(), getConf());

            if (!reader.next(key, vector)) {
                log.warn("No input vectors found in ", p);
                return 0;
            }

            final int cardinality = vector.get().size();
            final List<Cluster> result = new java.util.ArrayList<Cluster>();
            List<Cluster> more;
            IndexClusterer clusterer = new IndexClusterer(cardinality);
            log.info("Starting clustering...");
            {
                do {
                    more = clusterer.add(vector.get());
                    if (more != null) result.addAll(more);
                } while (reader.next(key, vector));
                result.addAll(clusterer.clusters());
            }

            log.info("re-clustering remaining vectors...");
            {
                IndexClusterer restClusterer = new IndexClusterer(vector.get().size());
                for (Vector v : clusterer.rest()) {
                    more = restClusterer.add(v);
                    if (more != null) result.addAll(more);
                }
                result.addAll(restClusterer.clusters());
            }
        }
        finally {
            IOUtils.closeStream(reader);
        }
        return 0;
    }

    final List<Cluster> merge(List<Cluster> result) {
        log.info("Starting meta-clustering...");
        final int cardinality = result.get(0).medoid().size();
        final Map<Vector, Cluster> sources = new java.util.HashMap<Vector, Cluster>(result.size());
        final IndexClusterer merger = new IndexClusterer(cardinality);
        final List<Cluster> metaClusters = new java.util.ArrayList<Cluster>();
        List<Cluster> more;
        for (Cluster c : result) {
            sources.put(c.medoid(), c);
            more = merger.add(c.medoid());
            if (more != null) metaClusters.addAll(more);
        }
        metaClusters.addAll(merger.clusters());

        final Histogram histogram = new Histogram();
        List<Cluster> flatClusters = new java.util.ArrayList<Cluster>();
        for (final Cluster meta : metaClusters) {
            final Vector medoid = meta.medoid();
            final List<Vector> related = new java.util.ArrayList<Vector>();
            final List<Double> similarities = new java.util.ArrayList<Double>();
            related.addAll(sources.get(medoid).related());
            similarities.addAll(sources.get(medoid).similarity());

            int i = 0;
            for (final Vector substitue : meta.related()) {
                related.add(substitue);
                similarities.add(meta.similarity().get(i));
                related.addAll(sources.get(substitue).related());
                // :TODO: we should recompute these similarities to the new medoid.
                similarities.addAll(sources.get(substitue).similarity());
                ++i;
            }
            histogram.add(related.size(), related.size());
            flatClusters.add(new Cluster(medoid, related, similarities));
        }
        return flatClusters;
    }

}
