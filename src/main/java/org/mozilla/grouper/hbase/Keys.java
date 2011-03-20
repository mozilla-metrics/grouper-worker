package org.mozilla.grouper.hbase;

import org.mozilla.grouper.model.CollectionRef;
import org.mozilla.grouper.model.Document;
import org.mozilla.grouper.model.DocumentRef;

/** Generates rowkeys. Instances must be thread-safe. */
public abstract class Keys {

    protected abstract String document(String ns, String ck, String docId);
    protected abstract String cluster(String ns, String ck, long rebuildTS, String label);
    protected abstract String allClusters(String ns, String ck, long rebuildTS);
    protected abstract String collection(String ns, String ck);
    
    String key(DocumentRef ref) {
        CollectionRef c = ref.ownerRef(); 
        return document(c.namespace(), c.key(), ref.id());
    }

    String key(CollectionRef ref) {
        return collection(ref.namespace(), ref.key());
    }
    
    final String key(Document doc) { return key(doc.ref()); }

}
