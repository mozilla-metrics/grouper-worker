package org.mozilla.grundle.clusterd;

import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import java.util.concurrent.TimeUnit;

import org.mozilla.grundle.input.Opinions;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakObject;


public class Importer {
    
    static final int BATCH_SIZE = 1000;
    
    private final RiakClient client;
    public Importer(final String url) { client = new RiakClient(url); }

    public void load(final InputStream in) {
        final ExecutorService pool =
            new ThreadPoolExecutor(
                    256, 256, 90, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), 
                    new RejectedExecutionHandler() {
                        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
                            try { executor.getQueue().put(task); } 
                            catch (InterruptedException e) { return; }
                        }
                    }
            );
        
        final Iterator<RiakObject> opinions = new Opinions().byTypeByVersionByProduct(in);
        int i = 0;
        while (opinions.hasNext()) {
            final RiakObject opinion = opinions.next();
            pool.execute(new Handler(client, opinion));
            if (i % 100 == 0) System.out.print(".");
            if (++i % 5000 == 0) System.out.printf("queued %s messages\n", i);
        } 
        shutdownGracefully(pool);
    }
    
    private void shutdownGracefully(final ExecutorService pool) {
        pool.shutdown();
        try {
            if (pool.awaitTermination(60, TimeUnit.SECONDS)) return;
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

    class Handler implements Runnable {
        private final RiakObject object;
        private final RiakClient client;
        Handler(final RiakClient client, final RiakObject object) { 
            this.object = object; 
            this.client = client; 
        }
        public void run() { client.store(object); }
    }

}
