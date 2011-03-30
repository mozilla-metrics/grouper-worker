package org.mozilla.grouper.input;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Opinions implements Iterable<String[]> {

    private static final Logger log = LoggerFactory.getLogger(Opinions.class);

    static enum Field {
        ID(0), TIMESTAMP(1), TYPE(2), PRODUCT(3), VERSION(4), PLATFORM(5), LOCALE(6),
        MANUFACTURER(7), DEVICE(8), URL(9), TEXT(10);
        public int i;
        Field(int c) { i = c; }
    }

    private final InputStream in_;

    public Opinions(final InputStream in) { in_ = in; }

    @Override
    public Iterator<String[]> iterator() {

        final TsvReader tsv = new TsvReader(in_);

        return new Iterator<String[]>() {

            int i = 0;
            String[] row;
            @Override
            public String[] next() {
                String[] r = row;
                row = null;
                return r;
            }

            @Override
            public boolean hasNext() {
                if (row != null) return true;
                try {
                    row = tsv.nextRow();
                    if (row == null) return false;
                    if (row.length != Field.values().length) {
                        log.warn("L{} skipping record (wrong number of columns) {}\n",
                                 i, Arrays.toString(row));
                        ++i;
                        next();
                        return hasNext();
                    }
                    ++i;
                }
                catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                return true;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}
