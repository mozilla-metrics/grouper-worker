package org.mozilla.grouper.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.mozilla.grouper.base.Assert;
import org.mozilla.grouper.hbase.Schema.Clusters;
import org.mozilla.grouper.hbase.Schema.Collections;
import org.mozilla.grouper.hbase.Schema.Documents;
import org.mozilla.grouper.model.Cluster;
import org.mozilla.grouper.model.Collection;
import org.mozilla.grouper.model.Collection.Attribute;
import org.mozilla.grouper.model.CollectionRef;
import org.mozilla.grouper.model.Document;
import org.mozilla.grouper.model.DocumentRef;
import org.mozilla.grouper.model.Model;


class Adapters {

  @SuppressWarnings("unchecked") static public <S extends Model>
  RowAdapter<S> create(final Factory factory, final Class<S> model) {
    if (model == Cluster.class) {
      return (RowAdapter<S>) new ClusterAdapter(factory);
    }
    if (model == Collection.class) {
      return (RowAdapter<S>) new CollectionAdapter(factory);
    }
    if (model == Document.class) {
      return (RowAdapter<S>) new DocumentAdapter(factory);
    }
    return Assert.unreachable(RowAdapter.class);
  }


  static public class ClusterAdapter implements RowAdapter<Cluster> {

    final Factory factory_;

    public
    ClusterAdapter(Factory factory) {
      factory_ = factory;
    }

    @Override public
    Put put(Cluster cluster) {
      final CollectionRef owner = cluster.ref().ownerRef();
      final Put put = new Put(Bytes.toBytes(key(cluster)));

      // Meta information
      put
      .add(Clusters.Main.FAMILY,
           Clusters.Main.NAMESPACE.qualifier,
           Bytes.toBytes(owner.namespace()))
      .add(Clusters.Main.FAMILY,
           Clusters.Main.KEY.qualifier,
           Bytes.toBytes(owner.key()))
      .add(Clusters.Main.FAMILY,
           Clusters.Main.TIMESTAMP.qualifier,
           Bytes.toBytes(cluster.ref().rebuildTs()))
      .add(Clusters.Main.FAMILY,
           Clusters.Main.LABEL.qualifier,
           Bytes.toBytes(cluster.ref().label()));

      // Medoid
      put
      .add(Clusters.Documents.FAMILY,
           Bytes.toBytes(cluster.representativeDoc().id()),
           Bytes.toBytes(1.0));

      // Contents
      int i = 0;
      for (DocumentRef doc : cluster.relatedDocs()) {
        put
        .add(Clusters.Documents.FAMILY,
             Bytes.toBytes(doc.id()),
             Bytes.toBytes(cluster.similarities().get(i).toString()));
        ++i;
      }

      return put;
    }

    @Override
    public String key(Cluster cluster) {
      return factory_.keys().key(cluster);
    }
  }


  static
  class DocumentAdapter implements RowAdapter<Document> {

    final Factory factory_;
    public DocumentAdapter(Factory factory) { factory_ = factory; }

    @Override
    public Put put(Document doc) {
      if (doc.text().length() == 0) return null;
      CollectionRef owner = doc.ref().ownerRef();
      return
      new Put(Bytes.toBytes(key(doc)))
      .add(Documents.Main.FAMILY,
           Documents.Main.NAMESPACE.qualifier,
           Bytes.toBytes(owner.namespace()))
      .add(Documents.Main.FAMILY,
           Documents.Main.COLLECTION_KEY.qualifier,
           Bytes.toBytes(owner.key()))
      .add(Documents.Main.FAMILY,
           Documents.Main.ID.qualifier,
           Bytes.toBytes(doc.ref().id()))
      .add(Documents.Main.FAMILY,
           Documents.Main.TEXT.qualifier,
           Bytes.toBytes(doc.text()));
    }

    @Override
    public String key(Document doc) {
      return factory_.keys().key(doc);
    }
  }


  static
  class CollectionAdapter implements RowAdapter<Collection> {

    final Factory factory_;

    public
    CollectionAdapter(Factory factory) {
      factory_ = factory;
    }

    @Override
    public
    Put put(Collection collection) {
      CollectionRef ref = collection.ref();
      Put put = new Put(Bytes.toBytes(key(collection)))
      .add(Collections.Main.FAMILY,
           Collections.Main.NAMESPACE.qualifier,
           Bytes.toBytes(ref.namespace()))
      .add(Collections.Main.FAMILY,
           Collections.Main.KEY.qualifier,
           Bytes.toBytes(ref.key()));

      // Only attributes that were specified by the caller are stored.
      for (Attribute a : Collection.Attribute.values()) {
        if (collection.get(a) == null) continue;
        switch (a) {
          case MODIFIED:
            put.add(Collections.Main.FAMILY,
                    Collections.Main.SIZE.qualifier,
                    Bytes.toBytes(collection.get(a).toString()));
            break;
          case SIZE:
            put.add(Collections.Main.FAMILY,
                    Collections.Main.SIZE.qualifier,
                    Bytes.toBytes(collection.get(a).toString()));
            break;
          case REBUILT:
            put.add(Collections.Main.FAMILY,
                    Schema.qualifier(Collections.Main.CONFIGURATION,
                                     "DEFAULT:rebuilt"),
                    Bytes.toBytes(collection.get(a).toString()));
          case PROCESSED:
            // :TODO:
            break;
          default:
            Assert.unreachable("Unknown collection attribute: ", a.name());
        }
      }
      return put;
    }

    @Override
    public String key(Collection collection) {
      return factory_.keys().key(collection.ref());
    }
  }
}
