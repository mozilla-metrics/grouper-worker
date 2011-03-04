package org.mozilla.grouper.clusterd;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;

public class Manage {

    static private final String USAGE =
        "Usage: java -jar clusterd.jar [(URL (import | list <bucket>)) | help]\n" +
        "   URL     points to a riak REST interface (such as http://localhost:8098/riak)" + 
        "   import  read (opinion) data from stdin\n" +
        "   list    prints the contents of the given bucket\n" + 
        "   help    print this message and exit";
   
    static private void exit(String message, int status) {
        (status == 0 ? System.out : System.err).println(message);
        System.exit(status);
    }
    
    static public void main(final String[] arguments) {
        if (arguments.length > 0 && "help".equals(arguments[0])) exit(USAGE, 0);
        if (arguments.length < 2) exit(USAGE, 1);
        final String url = arguments[0];
        final String command = arguments[1];
        
        if ("import".equals(command)) new Importer(url).load(System.in);
        else if ("list".equals(command) && arguments.length >= 3) list(url, arguments[2]);
        else exit(USAGE, 1);
    }
    
    static public void list(final String url, final String bucket) {
        final RiakClient riak = new RiakClient(url);
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
    }
}
