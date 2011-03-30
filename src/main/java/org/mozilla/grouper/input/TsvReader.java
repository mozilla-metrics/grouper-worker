package org.mozilla.grouper.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

public class TsvReader {

    static private final Charset UTF8 = Charset.forName("UTF-8");

    private final BufferedReader reader;

    private boolean escaped = false;

    private final StringBuilder builder = new StringBuilder();


    public TsvReader(final InputStream in) {
        reader = new BufferedReader(new InputStreamReader(in, UTF8), 32768 * 32);
    }

    /** TSV reading state machine. opencsv does not support our format (escape without quotes). */
    public String[] nextRow() throws IOException {
        final List<String> row = new LinkedList<String>();
        char c;
        while (true) {
            int i = reader.read();
            if (i == -1) return null;
            c = (char) i;
            if (!escaped) {
                switch (c) {
                    case '\\':
                        escaped = true;
                        continue;
                    case '\t':
                        row.add(builder.toString());
                        builder.setLength(0);
                        continue;
                    case '\n':
                        row.add(builder.toString());
                        builder.setLength(0);
                        return row.toArray(new String[row.size()]);
                }
            }
            builder.append(c);
            escaped = false;
        }
    }
}
