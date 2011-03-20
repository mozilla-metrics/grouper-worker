package org.mozilla.grouper.manage;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.mozilla.grouper.base.Configuration;
import org.mozilla.grouper.hbase.Factory;
import org.mozilla.grouper.hbase.Importer;
import org.mozilla.grouper.input.Opinions;


public class Manager {

    static private final String USAGE =
        "Usage: java -jar grouperfish.jar [--config PATH] [import <ns> | list <ns> <ck> | help]\n" +
        "   load    read (opinion) data from stdin\n" +
        "   list    prints all documents of the given collection\n" + 
        "   help    print this message and exit";
   
    public Manager(Configuration conf) {
        conf_ = conf;
    }

    static private void exit(String message, int status) {
        (status == 0 ? System.out : System.err).println(message);
        System.exit(status);
    }
    
    static public void main(final String[] args) {
        List<String> arguments = Arrays.asList(args);
        
        String configFile = "grouperconf.json";
        int i = 0;
        {
            while (arguments.size() > i && arguments.get(i).startsWith("--")) {
                if ("--help".equals(arguments.get(i))) {
                    exit(USAGE, 0);
                }
                if ("--config".equals(arguments.get(i))) {
                    configFile = arguments.get(++i);
                }
                else {
                    exit(USAGE, 1);
                }
                ++i;
            }
            if (arguments.size() == i) exit(USAGE, 1);
        }
        final Configuration conf = Configuration.fromJsonFile(configFile);
        
        final String command = arguments.get(i);
        ++i;
        final List<String> cmdArgs = arguments.subList(i, arguments.size());
        
        if ("help".equals(command)) 
            exit(USAGE, 0);
        
        if ("import".equals(command) && cmdArgs.size() == 1)
            new Manager(conf).load(cmdArgs.get(0), System.in);
        
        else if ("list".equals(command) && cmdArgs.size() >= 2)
            new Manager(conf).list(cmdArgs.get(0), cmdArgs.get(1));
        
        else 
            exit(USAGE, 1);
    }
    
    private final Configuration conf_;
    
    public void list(String namespace, String collectionKey) {
        Factory f = new Factory(conf_);
        f.table("documents");
        // do something like this, for hbase....
        /*
        System.out.format("Bucket %s at %s\n", bucket, url);
        final BucketResponse r = riak.streamBucket(bucket);
        if (!r.isSuccess()) exit("Could not stream bucket!", 1);
        final RiakClient fetchClient =  new RiakClient(url);
        final RequestMeta meta = RequestMeta.readParams(1);
        for (String key : r.getBucketInfo().getKeys()) {
            final FetchResponse fetch = fetchClient.fetch(bucket, key, meta);
            if (!fetch.isSuccess()) exit("Error receiving value for key " + key, 1);
            System.out.format("%s: %s\n", key, fetch.getObject().getValue());
            fetch.close();
        }
        r.close();
        */
    }
    
    public void load(String namespace, InputStream in) {
        new Importer(new Factory(conf_)).load(new Opinions(namespace).byTypeByVersionByProduct(in));
    }
    
}
