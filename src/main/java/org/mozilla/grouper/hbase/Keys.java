package org.mozilla.grouper.hbase;

import org.mozilla.grouper.model.CollectionRef;
import org.mozilla.grouper.model.Document;
import org.mozilla.grouper.model.DocumentRef;

/**
 * Generates rowkeys. Instances must be thread-safe.
 * For now, keys will be UTF-8 coded. We'll probably need to allow any byte[] for keys for maximum
 * space efficiency.
 *
 * Whatever scheme is used to generate keys, it must work with the prefix methods defined here, to
 * make for efficient scans.
 */
public abstract class Keys {

    public String documentPrefix(CollectionRef col) {
        return document(col.namespace(), col.key(), null);
    }

    public String clustersPrefix(String ns, String ck, long rebuildTS) {
        return cluster(ns, ck, rebuildTS, null) + '/';
    }

    protected abstract String document(String ns, String ck, String docID);
    protected abstract String cluster(String ns, String ck, long rebuildTS, String label);
    protected abstract String collection(String ns, String ck);

    public final String key(DocumentRef ref) {
        CollectionRef c = ref.ownerRef();
        return document(c.namespace(), c.key(), ref.id());
    }

    public final String key(CollectionRef ref) {
        return collection(ref.namespace(), ref.key());
    }

    public final String key(Document doc) {
        return key(doc.ref());
    }

}
