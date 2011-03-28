package org.mozilla.grouper.model;

import java.util.List;

import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;


public class Cluster extends BaseCluster {
    private final ClusterRef ref_;
    private final List<DocumentRef> contents_ = new java.util.ArrayList<DocumentRef>();

    public Cluster(ClusterRef ref, Vector medoid, List<Vector> related, List<Double> similarities) {
        super(medoid, related, similarities);
        ref_ = ref;
    }

    public Cluster(ClusterRef ref, BaseCluster data) {
        super(data);
        ref_ = ref;
    }

    public ClusterRef ref() {
        return ref_;
    }

    public List<DocumentRef> contents() {
        if (contents_.isEmpty()) {
            CollectionRef owner_ = ref_.ownerRef();
            contents_.add(new DocumentRef(owner_, ((NamedVector) medoid()).getName()));
            for (Vector v : related()) {
                contents_.add(new DocumentRef(owner_, ((NamedVector) v).getName()));
            }
        }
        return contents_;
    }

}
