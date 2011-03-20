package org.mozilla.grouper.model;

public class CollectionRef {
 
    public String namespace() { return namespace_; }
    
    /** The collection-key (a <em>part</em> of rowkeys). */
    public String key() { return key_; }

    public CollectionRef(String namespace, String key) {
        namespace_ = namespace;
        key_ = key;
    }
    
    private final String key_;
    private final String namespace_;

}
