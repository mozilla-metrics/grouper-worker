package org.mozilla.grouper.jobs.textcluster;

import java.util.List;

import org.apache.mahout.math.Vector;

/** :TOOO: maybe move to the model */
public class Cluster {
    private final Vector medoid_;
    private final List<Vector> related_;
    private final List<Double> similarity_;

    public Cluster(Vector medoid, List<Vector> related, List<Double> similarity) {
        medoid_ = medoid;
        related_ = related;
        similarity_ = similarity;
    }

    public Vector medoid() { return medoid_; }
    public int size() { return related_.size(); }
    public final List<Vector> related() { return related_; };
    public final List<Double> similarity() { return similarity_; };

}
