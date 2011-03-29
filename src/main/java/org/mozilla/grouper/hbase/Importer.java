package org.mozilla.grouper.hbase;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
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

    private static final Logger log = LoggerFactory.getLogger(Importer.class);
    private static final int BATCH_SIZE = 1000;

    private String tableName_;
    private final Factory factory_;

    // The pool adds references here, helping to free tables later on.
    final List<HTableInterface> tables_ = new java.util.LinkedList<HTableInterface>();


    public Importer(final Factory factory, final String tableName) {
        tableName_ = tableName;
        factory_ = factory;
    }

    public void load(Iterable<T> input) {

        log.debug("Starting import into table {}", tableName_);

        final ExecutorService workers = workers();
        int i = 1; // != 0 so % does not hit right away
        List<T> batch = new ArrayList<T>(BATCH_SIZE);
        for (T item : input) {
            // :TODO: predicate pushdown
            // if (doc.text().length() == 0) continue;
            batch.add(item);
            if (i % BATCH_SIZE == 0) {
                workers.submit(new Insert(batch));
                batch = new ArrayList<T>(BATCH_SIZE);
            }
            if (i % 50000 == 0) log.debug("Queued {} messages for table {}", i, tableName_);
            ++i;
        }
        log.info(String.format("Inserting batch of size %d", batch.size()));
        workers.submit(new Insert(batch));
        shutdownGracefully(workers);

        // now the tables of all threads are safe to discard:
        for (HTableInterface t: tables_) factory_.release(t);
    }

    class Insert implements Runnable {
        private final List<T> items_;
        private final RowAdapter<T> adapter_;
        Insert(final List<T> items) {
            items_ = items;
            adapter_ = items_.size() > 0 ? Adapters.create(factory_, items.get(0)) : null;
        }
        @Override
        public void run() {
            if (items_.size() == 0) return;
            List<Put> batch = new ArrayList<Put>(items_.size());
            // adapter_ = Adapters.create(factory_, items_.get(0));
            for (T item : items_) {
                Adapters.create(factory_, item).put(item);
                batch.add(adapter_.put(item));
            }
            try {
                table_.get().put(batch);
            }
            catch (IOException e) {
                String from = adapter_.key(items_.get(0));
                String to = adapter_.key(items_.get(items_.size() - 1));
                log.error(String.format("While inserting batch %s,%s", from, to));
                log.error("IO Error in importer", e);
            }
        }
    }

    /**
     * So there is this factory where all workers do is running and then relax at the
     * pool, and where all clients must wait in a queue.
     * It is a pretty fun work environment... wait until everyone gets garbage collected though.
     */
    private ExecutorService workers() {
        return new ThreadPoolExecutor(
           10, 20, 90, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100),
           new ThreadFactory() {
               @Override
               public Thread newThread(final Runnable r) {
                   final HTableInterface workerTable = factory_.table(tableName_);
                   tables_.add(workerTable);
                   Thread worker = new Thread(r) {
                       @Override
                       public void run() {
                           table_.set(workerTable);
                           super.run();
                       }
                   };
                   worker.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                       @Override
                       public void uncaughtException(Thread t, Throwable e) {
                           log.error("Uncaught exception from importer worker.", e);
                       }
                   });
                   return worker;
               }
           },
           new RejectedExecutionHandler() {
               /** Wait in the queue for the next execution. */
               @Override
               public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
                   try { executor.getQueue().put(task); }
                   catch (InterruptedException e) {throw new RuntimeException(e); }
               }
           }
       );
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

}
