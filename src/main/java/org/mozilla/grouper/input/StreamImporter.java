package org.mozilla.grouper.input;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.mozilla.grouper.base.Config;
import org.mozilla.grouper.hbase.Factory;
import org.mozilla.grouper.input.Opinions.Field;
import org.mozilla.grouper.model.Collection;
import org.mozilla.grouper.model.Collection.Attribute;
import org.mozilla.grouper.model.CollectionRef;
import org.mozilla.grouper.model.Document;
import org.mozilla.grouper.model.DocumentRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamImporter {

    private static final Logger log = LoggerFactory.getLogger(StreamImporter.class);

    private final Factory factory_;
    private final InputStream in_;
    private final String namespace_;
    private final KeyGen[] keyGens_;

    public StreamImporter(final Config conf,
                          final InputStream in,
                          final String namespace,
                          final String[] keyPatterns) {
        factory_ = new Factory(conf);
        in_ = in;
        namespace_ = namespace;
        keyGens_ = new KeyGen[keyPatterns.length];
        for (int i = keyPatterns.length; i --> 0;) keyGens_[i] = new KeyGen(keyPatterns[i]);

    }

    static enum Counters { ROWS_USED, ROWS_DISCARDED, DOCS_CREATED, COLLECTIONS_CREATED };

    public int[] load() {

        final Map<String, CollectionRef> collectionRefs = new java.util.HashMap<String, CollectionRef>();
        final Map<String, Integer> sizes = new java.util.HashMap<String, Integer>();
        DocumentsGenerator gen = new DocumentsGenerator(collectionRefs, sizes);
        factory_.importer(Document.class).load(gen);

        int[] counters = gen.counters;

        List<Collection> collections = new java.util.ArrayList<Collection>();
        for (CollectionRef ref : collectionRefs.values()) {
            collections.add(new Collection(ref).set(Attribute.SIZE,
                                                    sizes.get(ref.key()).longValue()));
        }
        factory_.importer(Collection.class).load(collections);
        return counters;
    }


    /**
     * Generates documents for all collections applicable to an input opinion.
     * Also maintains the list of collections (in place) so we import those as well.
     */
    class DocumentsGenerator implements Iterable<Document> {
        final int[] counters = new int[Counters.values().length];
        final Map<String, CollectionRef> collectionRefs_;
        final Map<String, Integer> sizes_;
        DocumentsGenerator(final Map<String, CollectionRef> collectionRefs,
                           final Map<String, Integer> sizes) {
            this.collectionRefs_ = collectionRefs;
            this.sizes_ = sizes;
        }
        @Override
        public Iterator<Document> iterator() {
            return new Iterator<Document>() {
                final Iterator<String[]> rows = new Opinions(in_).iterator();
                int k = keyGens_.length;
                String[] row = null;
                @Override
                public Document next() {
                    if (++k >= keyGens_.length) {
                        do row = rows.next(); while (!isOk(row));
                        k = 0;
                    }
                    String key;
                    do key = keyGens_[k++].key(row); while (k < keyGens_.length && key == null);
                    if (key == null) {
                        log.warn("Could not associate opinion {} to any collection!", row);
                        return next();
                    }

                    if (!collectionRefs_.containsKey(key))
                        collectionRefs_.put(key, new CollectionRef(namespace_, key));
                    sizes_.put(key, sizes_.get(key)+1);
                    DocumentRef ref = new DocumentRef(collectionRefs_.get(key),
                                                      row[Field.ID.i]);
                    return new Document(ref, row[Field.TEXT.i]);
                }
                @Override
                public boolean hasNext() { return rows.hasNext(); }
                @Override
                public void remove() { throw new UnsupportedOperationException(); }

                private boolean isOk(String[] row) {
                    if (row[Field.TEXT.i].length() == 0) {
                        counters[Counters.ROWS_DISCARDED.ordinal()]++;
                        return false;
                    }
                    counters[Counters.ROWS_USED.ordinal()]++;
                    return true;
                }
            };
        }
    }

    /**
     * Generates collection keys from patterns. Patterns are strings that use field names like
     * variables. Examples:
     * Note that there is no escape syntax.
     * "$PRODUCT:$VERSION-$PLATFORM-$TYPE" will produce keys such as "firefox:4.0b12-mac-issue".
     * If an opinion does not contain the required information for all flags, make will return
     * null for this pattern.
     */
    private static class KeyGen {
        //private static final Logger log = LoggerFactory.getLogger(KeyGen.class);
        final String[] fragments_;
        final Field[] fields_;

        KeyGen (String pattern) {
            List<String> fragments = new ArrayList<String>(); // Contains a null where a field should be used.
            List<Field> fields = new ArrayList<Opinions.Field>(); // Contains each field to be used.
            Matcher matcher = Pattern.compile("^[^$]*[$]([a-zA-Z0-9]+)[^$]*").matcher(pattern);
            while (matcher.find()) {
                StringBuffer tmp = new StringBuffer();
                matcher.appendReplacement(tmp, "");
                if (tmp.length() > 0) {
                    fields.add(null);
                    fragments.add(tmp.toString());
                }
                fields.add(Field.valueOf(matcher.group()));
            }
            fragments_ = fragments.toArray(new String[fragments.size()]);
            fields_ = fields.toArray(new Field[fields.size()]);
        }

        String key(String[] opinion) {
            StringBuilder sb = new StringBuilder();
            int frag = 0;
            for (Field field : fields_) {
                if (field == null) sb.append(fragments_[frag++]);
                else sb.append(field);
            }
            return sb.toString();
        }

    }

}
