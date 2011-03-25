package org.mozilla.grouper.hbase;


public class ReversePartsKeys extends Keys {

    private StringBuilder revAppend(StringBuilder sb, String part) {
        for (int i = part.length(); i --> 0; ) sb.append(part.charAt(i));
        return sb;
    }

    @Override
    protected String document(String ns, String ck, String docID) {
        if (docID == null) docID = "";
        int size = ns.length() + ck.length() + docID.length() + 2;
        return revAppend(new StringBuilder(size).append(ns).append('/').append(ck).append('/'),
                         docID).toString();
    }

    @Override
    protected String cluster(String ns, String ck, long rebuildTS, String label) {
        if (label == null) label = "";
        String ts = Long.valueOf(rebuildTS).toString();
        int size = ns.length() + 1 + ck.length() + 1 + ts.length() + 1 + label.length();
        return revAppend(new StringBuilder(size).append(ns).append('/').append(ck).append('/'),
                         ts).append('/').append(label).toString();
    }

    @Override
    protected String collection(String ns, String ck) {
        int size = ns.length() + ck.length() + 1;
        return new StringBuilder(size).append(ns).append('/').append(ck).toString();
    }

}
