package org.mozilla.grundle.input;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;

import com.basho.riak.client.RiakObject;


public class Opinions {

    static enum Field {
        ID(0), TIMESTAMP(1), TYPE(2), PRODUCT(3), VERSION(4), PLATFORM(5), LOCALE(6),
        MANUFACTURER(7), DEVICE(8), URL(9), TEXT(10);
        public int i;
        Field(int c) { i = c; }
    }
    static private final Charset UTF8 = Charset.forName("UTF-8");
    
    public Iterator<RiakObject> byTypeByVersionByProduct(final InputStream in) {
        return new TemplateIterator(in) {
            String bucket(String[] row) {
                return String.format("input,%s,%s,%s", 
                                     row[Field.PRODUCT.i], row[Field.VERSION.i], row[Field.TYPE.i]);
            }
        };
    }
    
    private abstract class TemplateIterator implements Iterator<RiakObject> {
        private final Iterator<String[]> rows;
        public TemplateIterator(final InputStream in) {
            rows = rows(in);
        }
        abstract String bucket(String[] row);
        public RiakObject next() {
            final String[] row = rows.next();
            return new RiakObject(bucket(row), row[Field.ID.i], row[Field.TEXT.i].getBytes(UTF8));
        }        
        public boolean hasNext() {
            return rows.hasNext();
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public Iterator<String[]> rows(final InputStream in) {

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
                        System.err.format("Line %s: weird record> %s\n", ++i, Arrays.toString(row));
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
