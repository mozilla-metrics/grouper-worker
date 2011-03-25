package org.mozilla.grouper.hbase;

import static org.mozilla.grouper.hbase.Schema.CF_CONTENT;
import static org.mozilla.grouper.hbase.Schema.COLLECTION_KEY;
import static org.mozilla.grouper.hbase.Schema.ID;
import static org.mozilla.grouper.hbase.Schema.NAMESPACE;
import static org.mozilla.grouper.hbase.Schema.TEXT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
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

    // Set on worker creation. Reused by insert-tasks.
    private final ThreadLocal<HTableInterface> table_ = new ThreadLocal<HTableInterface>();

    private static final Logger LOG = LoggerFactory.getLogger(Importer.class);
    private static final int BATCH_SIZE = 1000;

    private final Factory factory_;
    private final Keys keys_;


    public Importer(final Factory factory) {
        factory_ = factory;
        keys_ = factory.keys();
    }

    public void load(Iterable<Document> docs) {
        // To ensure HTables are freed after the import is done.
        final List<HTableInterface> tables_ = new java.util.LinkedList<HTableInterface>();

        final ExecutorService pool = new ThreadPoolExecutor(
                10, 20, 90, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        final HTableInterface workerTable = factory_.table("documents");
                        tables_.add(workerTable);
                        return new Thread(r) {
                            @Override
                            public void run() {
                                table_.set(workerTable);
                                super.run();
                            }
                        };
                    }
                },
                // block on task queue insert
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
                        try { executor.getQueue().put(task); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    }
                }
        );

        int i = 1; // != 0 so % does not hit right away
        List<Document> batch = new ArrayList<Document>(BATCH_SIZE);
        for (Document doc : docs) {
            if (doc.text().length() == 0) continue;
            batch.add(doc);
            if (i % BATCH_SIZE == 0) {
                pool.submit(new Insert(batch));
                batch = new ArrayList<Document>(BATCH_SIZE);
            }
            if (i % 1000 == 0) System.out.print(".");
            if (i % 10000 == 0) System.out.printf("queued %s messages\n", i);
            ++i;
        }
        pool.submit(new Insert(batch));
        shutdownGracefully(pool);

        // now the tables of all threads are safe to discard:
        for (HTableInterface t: tables_) factory_.release(t);
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



    class Insert implements Runnable {
        private final List<Document>  docs_;
        Insert(final List<Document>  docs) { docs_ = docs; }
        @Override
        public void run() {
            List<Put> batch = new ArrayList<Put>(docs_.size());
            for (Document doc : docs_) {
                batch.add(new Put(Bytes.toBytes(keys_.key(doc)))
                          .add(CF_CONTENT, NAMESPACE, Bytes.toBytes(doc.ref().ownerRef().namespace()))
                          .add(CF_CONTENT, COLLECTION_KEY, Bytes.toBytes(doc.ref().ownerRef().key()))
                          .add(CF_CONTENT, ID, Bytes.toBytes(doc.ref().id()))
                          .add(CF_CONTENT, TEXT, Bytes.toBytes(doc.text())));
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
