package org.mozilla.grouper.hbase;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Parallel importer of stuff into Hbase.
 */
public class Importer<T> {

    // Set on worker creation. Reused by insert-tasks.
    private final ThreadLocal<HTableInterface> table_ = new ThreadLocal<HTableInterface>();

    private static final Logger LOG = LoggerFactory.getLogger(Importer.class);
    private static final int BATCH_SIZE = 1000;

    private String tableName_;
    private final Factory factory_;


    public Importer(final Factory factory, final String tableName) {
        tableName_ = tableName;
        factory_ = factory;
    }

    public void load(Iterable<T> input) {
        // To ensure HTables are freed after the import is done.
        final List<HTableInterface> tables_ = new java.util.LinkedList<HTableInterface>();

        final ExecutorService pool = new ThreadPoolExecutor(
                10, 20, 90, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        final HTableInterface workerTable = factory_.table(tableName_);
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
        List<T> batch = new ArrayList<T>(BATCH_SIZE);
        for (T item : input) {
            // :TODO: predicate pushdown
            // if (doc.text().length() == 0) continue;
            batch.add(item);
            if (i % BATCH_SIZE == 0) {
                pool.submit(new Insert(batch));
                batch = new ArrayList<T>(BATCH_SIZE);
            }
            if (i %  2500 == 0) System.out.print(".");
            if (i % 50000 == 0) System.out.printf("queued %s messages\n", i);
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
        private final List<T> items_;
        private final RowAdapter<T> adapter_;
        Insert(final List<T> items) {
            items_ = items;
            adapter_ = Adapters.create(factory_, items.get(0));
        }
        @Override
        public void run() {
            List<Put> batch = new ArrayList<Put>(items_.size());
            for (T item : items_) batch.add(adapter_.put(item));
            try { table_.get().put(batch); }
            catch (IOException e) {
                String from = adapter_.key(items_.get(0));
                String to = adapter_.key(items_.get(items_.size() - 1));
                LOG.error(String.format("While inserting batch %s,%s", from, to));
                LOG.error("IO Error in importer", e);
            }
        }
    }


}
