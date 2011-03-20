package org.mozilla.grouper.hbase;


public class SimpleKeys extends Keys {

    protected String document(String ns, String ck, String docId) {
        int size = ns.length() + ck.length() + docId.length() + 2;
        return new StringBuilder(size).append(ns).append('/')
                                      .append(ck).append('/')
                                      .append(docId).toString();
    }
    
    protected String cluster(String ns, String ck, long rebuildTS, String label) {
        String ts = Long.valueOf(rebuildTS).toString();
        int size = ns.length() + ck.length() + label.length() + ts.length() + 3;
        return new StringBuilder(size).append(ns).append('/')
                                      .append(ck).append('/')
                                      .append(label).append('/')
                                      .append(ts).toString();
    }
    
    protected String allClusters(String ns, String ck, long rebuildTS) {
        String ts = Long.valueOf(rebuildTS).toString();
        int size = ns.length() + ck.length() + ts.length() + 1;
        return new StringBuilder(size).append(ns).append('/')
                                      .append(ts).append('/')
                                      .append(ck).toString();
    }
    
    protected String collection(String ns, String ck) {
        int size = ns.length() + ck.length() + 1;
        return new StringBuilder(size).append(ns).append('/')
                                      .append(ck).toString();
    }


}
