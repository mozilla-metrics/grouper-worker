package org.mozilla.grouper.model;

public class ClusterRef {

    public CollectionRef ownerRef() { return ownerRef_; }

    /** The cluster label, a part of the row key. */
    public String label() { return label_; }

    /**
     * Time when the full rebuild was started that created this cluster
     * (or onto which this cluster was added).
     */
    public long rebuildTs() { return rebuildTs_;}

    public ClusterRef(CollectionRef ownerRef, long rebuildTs, String label) {
        ownerRef_ = ownerRef;
        label_ = label;
        rebuildTs_ = rebuildTs;
    }

    private final long rebuildTs_;
    private final String label_;
    private final CollectionRef ownerRef_;

}
