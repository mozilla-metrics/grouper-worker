package org.mozilla.grouper.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.mozilla.grouper.model.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Parallel importer of stuff into Hbase.
 */
public class Importer {
    
    static final Logger LOG = LoggerFactory.getLogger(Importer.class); 

    private static final int BATCH_SIZE = 20;
    private final Factory factory_;    
    private final Keys keys_;
    
    public Importer(final Factory factory) { 
        factory_ = factory; 
        keys_ = factory.keys(); 
    }

    public void load(Iterable<Document> docs) {
        
        final ExecutorService pool =
            new ThreadPoolExecutor(
                    16, 16, 90, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                    /** block on task queue insert */
                    new RejectedExecutionHandler() {
                        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
                            try { executor.getQueue().put(task); } 
                            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                        }
                    }
            );

        int i = 1; // yes, one (so our stepping works)
        List<Document> batch = new ArrayList<Document>(BATCH_SIZE); 
        for (Document doc : docs) {
            batch.add(doc);
            if (i % BATCH_SIZE == 0) { 
                pool.execute(new Insert(batch));
                batch = new ArrayList<Document>(BATCH_SIZE);
            }
            if (i % 100 == 0) System.out.print(".");
            if (i % 5000 == 0) System.out.printf("queued %s messages\n", i);
            ++i;
        } 
        pool.execute(new Insert(batch));
        shutdownGracefully(pool);
    }
    
    private void shutdownGracefully(final ExecutorService pool) {
        pool.shutdown();
        try {
            if (pool.awaitTermination(120, TimeUnit.SECONDS)) return;
            pool.shutdownNow();
            if (pool.awaitTermination(60, TimeUnit.SECONDS)) return;
            System.err.println("Importer pool did not terminate within timeout.");
            System.exit(1);
        }
        catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    final byte[] CONTENT = Bytes.toBytes("content");
    final byte[] NAMESPACE = Bytes.toBytes("namespace");
    final byte[] COLLECTION_KEY = Bytes.toBytes("collectionKey");
    final byte[] ID = Bytes.toBytes("id");
    final byte[] TEXT = Bytes.toBytes("text");
    class Insert implements Runnable {

        private final ThreadLocal<HTableInterface> table_ = new ThreadLocal<HTableInterface>() {
            protected HTableInterface initialValue() {
                return factory_.table("documents");
            };
        };
        private final List<Document>  docs_;
        Insert(final List<Document>  docs) { docs_ = docs; }
        public void run() {
            List<Put> batch = new ArrayList<Put>(docs_.size());
            for (Document doc : docs_) {
                byte[] ns = Bytes.toBytes(doc.ref().ownerRef().namespace());
                byte[] rowKey = Bytes.toBytes(keys_.key(doc));
                byte[] ck = Bytes.toBytes(doc.ref().ownerRef().key());
                byte[] id = Bytes.toBytes(doc.ref().id());
                byte[] text = Bytes.toBytes(doc.text());
                new Put(rowKey).add(CONTENT, NAMESPACE, ns)
                               .add(CONTENT, COLLECTION_KEY, ck)
                               .add(CONTENT, ID, id)
                               .add(CONTENT, TEXT, text);
            }
            try {
                table_.get().put(batch);
            } catch (IOException e) {
                String from = keys_.key(docs_.get(0));
                String to = keys_.key(docs_.get(docs_.size() - 1));
                LOG.error(String.format("While inserting batch %s,%s", from, to));
                LOG.error("IO Error in importer", e);
            }
        }
    }

}
