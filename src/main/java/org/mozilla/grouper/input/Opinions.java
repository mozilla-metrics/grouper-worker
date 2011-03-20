package org.mozilla.grouper.input;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

import org.mozilla.grouper.model.CollectionRef;
import org.mozilla.grouper.model.Document;
import org.mozilla.grouper.model.DocumentRef;


public class Opinions {
    
    static enum Field {
        ID(0), TIMESTAMP(1), TYPE(2), PRODUCT(3), VERSION(4), PLATFORM(5), LOCALE(6),
        MANUFACTURER(7), DEVICE(8), URL(9), TEXT(10);
        public int i;
        Field(int c) { i = c; }
    }
    
    public Opinions(String namespace) {
        namespace_ = namespace;
    }
    
    public Iterable<Document> byTypeByVersionByProduct(final InputStream in) {
        return new BaseIterable(in) {
            CollectionRef collectionRef(String[] row) {
                String key = String.format("%s/%s/%s", row[Field.PRODUCT.i], row[Field.VERSION.i], row[Field.TYPE.i]);
                return new CollectionRef(namespace_, key);
            }
        };
    }
    
    private abstract class BaseIterable implements Iterable<Document> {
        abstract CollectionRef collectionRef(String[] row);
        BaseIterable(InputStream in) { rows_ = rows(in); }
        public Iterator<Document> iterator() {
            return new Iterator<Document>() {
                public Document next() {
                    final String[] row = rows_.next();
                    return new Document(new DocumentRef(collectionRef(row), row[Field.ID.i]), row[Field.TEXT.i]);
                }        
                public boolean hasNext() { return rows_.hasNext(); }
                public void remove() { throw new UnsupportedOperationException(); }
            };
        }
        private final Iterator<String[]> rows_;
    }

    private final String namespace_;

    private Iterator<String[]> rows(final InputStream in) {

        final TsvReader tsv = new TsvReader(in);

        return new Iterator<String[]>() {

            int i = 0;
            String[] row;
            public String[] next() {
                String[] r = row;
                row = null;
                return r;
            }
        
            public boolean hasNext() {
                if (row != null) return true;
                try {
                    row = tsv.nextRow();
                    if (row == null) return false;
                    if (row.length != Field.values().length) {
                        System.err.format("L%s weird record: %s\n", ++i, Arrays.toString(row));
                        next();
                        return hasNext();
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                return true;
            }
    
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
