package org.mozilla.grouper.hbase;

import static org.mozilla.grouper.hbase.Schema.CF_CONTENT;
import static org.mozilla.grouper.hbase.Schema.CF_DOCUMENTS;
import static org.mozilla.grouper.hbase.Schema.CF_META;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.mozilla.grouper.base.Assert;
import org.mozilla.grouper.model.Cluster;
import org.mozilla.grouper.model.Collection;
import org.mozilla.grouper.model.Collection.Attribute;
import org.mozilla.grouper.model.CollectionRef;
import org.mozilla.grouper.model.Document;
import org.mozilla.grouper.model.DocumentRef;

public class Adapters {

    @SuppressWarnings("unchecked")
    static public <S> RowAdapter<S> create(Factory factory, S model) {
        if (model == null) return new RowAdapter<S>() {
                @Override public Put put(S item) { return Assert.unreachable(Put.class); }
                @Override public String key(S item) { return Assert.unreachable(String.class); }
        };
        if (model instanceof Cluster) return (RowAdapter<S>) new ClusterAdapter(factory);
        if (model instanceof Collection) return (RowAdapter<S>) new CollectionAdapter(factory);
        if (model instanceof Document) return (RowAdapter<S>) new DocumentAdapter(factory);
        return Assert.unreachable(RowAdapter.class);
    }


    static public class ClusterAdapter implements RowAdapter<Cluster> {

        final Factory factory_;
        public ClusterAdapter(Factory factory) { factory_ = factory; }

        @Override
        public Put put(Cluster cluster) {
            CollectionRef owner = cluster.ref().ownerRef();
            final Put put =
                new Put(Bytes.toBytes(key(cluster)))
                .add(CF_META, Schema.NAMESPACE, Bytes.toBytes(owner.namespace()))
                .add(CF_META, Schema.COLLECTION_KEY, Bytes.toBytes(owner.key()))
                .add(CF_META, Schema.LAST_REBUILD, Bytes.toBytes(cluster.ref().rebuildTs()))
                .add(CF_META, Schema.LABEL, Bytes.toBytes(cluster.ref().label()))
                .add(CF_DOCUMENTS,
                     Bytes.toBytes(cluster.representativeDoc().id()),
                     Bytes.toBytes(1.0));

            int i = 0;
            for (DocumentRef doc : cluster.relatedDocs()) {
                put.add(CF_DOCUMENTS,
                        Bytes.toBytes(doc.id()),
                        Bytes.toBytes(cluster.similarities().get(i).toString()));
                ++i;
            }
            return put;
        }

        @Override
        public String key(Cluster item) {
            CollectionRef c = item.ref().ownerRef();
            return factory_.keys().cluster(c.namespace(), c.key(),
                                           item.ref().rebuildTs(), item.ref().label());
        }
    }


    static class DocumentAdapter implements RowAdapter<Document> {

        final Factory factory_;
        public DocumentAdapter(Factory factory) { factory_ = factory; }

        @Override
        public Put put(Document doc) {
            CollectionRef owner = doc.ref().ownerRef();
            return
                new Put(Bytes.toBytes(key(doc)))
                .add(CF_CONTENT, Schema.NAMESPACE, Bytes.toBytes(owner.namespace()))
                .add(CF_CONTENT, Schema.COLLECTION_KEY, Bytes.toBytes(owner.key()))
                .add(CF_CONTENT, Schema.ID, Bytes.toBytes(doc.ref().id()))
                .add(CF_CONTENT, Schema.TEXT, Bytes.toBytes(doc.text()));
        }

        @Override
        public String key(Document doc) {
            CollectionRef c = doc.ref().ownerRef();
            return factory_.keys().document(c.namespace(), c.key(), doc.ref().id());
        }
    }


    static class CollectionAdapter implements RowAdapter<Collection> {

        final Factory factory_;
        public CollectionAdapter(Factory factory) { factory_ = factory; }

        @Override
        public Put put(Collection collection) {
            CollectionRef ref = collection.ref();
            Put put = new Put(Bytes.toBytes(key(collection)))
                      .add(CF_META, Schema.NAMESPACE, Bytes.toBytes(ref.namespace()))
                      .add(CF_META, Schema.COLLECTION_KEY, Bytes.toBytes(ref.key()));
            for (Attribute a : Collection.Attribute.values()) {
                if (collection.get(a) == null) continue;
                switch (a) {
                    case REBUILT:
                        // :TODO: get rid of the configuration CF
                        // :TODO: get rid of JSON for meta data (bad idea (tm)).
                        String json = String.format("{\"lastRebuild\": %d}", collection.get(a));
                        put.add(Schema.CF_CONFIGURATIONS,
                                Bytes.toBytes("DEFAULT"),
                                Bytes.toBytes(json));
                    break;
                    case SIZE:
                        put.add(CF_META, Schema.SIZE, Bytes.toBytes(collection.get(a).toString()));
                    break;
                    case MODIFIED:
                    default:
                        // :TODO: handle the other attributes
                        Assert.unreachable("not implemented");
                }
            }
            return put;
        }

        @Override
        public String key(Collection collection) {
            CollectionRef c = collection.ref();
            return factory_.keys().collection(c.namespace(), c.key());
        }
    }
}
