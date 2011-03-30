package org.mozilla.grouper.model;

public class DocumentRef {

    public CollectionRef ownerRef() { return ownerRef_; }
    public String id() { return id_; }

    public DocumentRef(CollectionRef ownerRef, String id) {
        ownerRef_ = ownerRef;
        id_ = id;
    }

    private final CollectionRef ownerRef_;
    private final String id_;

}
