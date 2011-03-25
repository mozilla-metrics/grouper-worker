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
    static final String[] STRINGS = new String[]{};

    final BufferedReader reader;


    static final int CHUNK = 32768;
    char[] buffer = new char[CHUNK];
    String[] row = null;
    boolean escaped = false;
    int pos = -1;
    int bytesRead = -1;
    final StringBuilder builder = new StringBuilder();


    public TsvReader(final InputStream in) {
        reader = new BufferedReader(new InputStreamReader(in, UTF8), CHUNK * 32);
    }

    /** TSV reading state machine. opencsv does not support our format (escape without quotes). */
    public String[] nextRow() throws IOException {
        final List<String> row = new LinkedList<String>();
        char c;
        while (true) {
            if (pos == bytesRead) {
                bytesRead = reader.read(buffer, 0, CHUNK);
                if (bytesRead == -1) return null;
                pos = 0;
            }
            c = buffer[pos++];
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
                        return row.toArray(STRINGS);
                }
            }
            builder.append(c);
            escaped = false;
        }
    }
}
