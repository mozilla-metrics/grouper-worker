package org.mozilla.grouper.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.mozilla.grouper.base.Config;
import org.mozilla.grouper.model.CollectionRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create (fresh) clusters from the given set of vectors and documents.
 */
public class CreateClusters extends AbstractCollectionTool {


    private static final Logger log = LoggerFactory.getLogger(CreateClusters.class);

    public static final String NAME = "cluster";

    public CreateClusters(Config conf, Configuration hadoopConf) {
        super(conf, hadoopConf);
    }

    @Override
    protected String name() { return NAME; }

    @Override
    protected int run(CollectionRef collection, long timestamp) throws Exception {
        Configuration hadoopConf = getConf();
        AbstractCollectionTool source = new VectorizeDocuments(conf_, hadoopConf);

        Path baseDir = outputDir(collection, timestamp);

        Path vectorsDir = new Path(source.outputDir(collection, timestamp), "tf-vectors");
        Path preClustersDir = new Path(baseDir, "canopy-clusters");
        // Path dest = outputDir(collection, timestamp);

        /*
        log.info("Generating canopies from {} to {}.",
                 vectorsDir, outputDir(collection, timestamp));

        final CanopyConfiguration config = new CanopyConfiguration(hadoopConf, vectorsDir, dest);
        CanopyClusterer algorithm = new CanopyClusterer(
        algorithm.run(g);

        */


        log.info("Starting canopy generation");
        CanopyDriver.run(hadoopConf, vectorsDir, preClustersDir, new ManhattanDistanceMeasure(),
                         50f, 30f, false, true);

        log.info("Starting kmeans generation");
        KMeansDriver.run(hadoopConf, preClustersDir, new Path(baseDir, "kmeans-clusters"),
                         new Path(baseDir, "output"), new EuclideanDistanceMeasure(),
                         0.001, 10, true, true);

        // From the mahout wiki:
        /*

        // now run the KMeansDriver job


        log.info("Starting kmeans generation");
        KMeansDriver driver;

        final KMeansConfiguration kMeansConfig =
            new KMeansConfiguration(hadoopConf, vectorsDir, dest, preClustersDir, 10);
        kMeansConfig.setDistanceMeasure(new CosineDistanceMeasure());
        kMeansConfig.setRunClustering(true);
        KMeansMapReduceAlgorithm algorithm = new KMeansMapReduceAlgorithm();
        algorithm.run(kMeansConfig);

         */

        return 0;
    }
}
