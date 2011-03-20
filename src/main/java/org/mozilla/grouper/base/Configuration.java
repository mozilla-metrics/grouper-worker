package org.mozilla.grouper.base;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

public class Configuration {

    public final String hbase() { return hbase_; }
    public final String redis() { return redis_; }
    public final String amqp() { return amqp_; }
    public final String prefix() { return prefix_; }

    public static final String DEFAULT_PREFIX = "gfish_";

    public static Configuration fromJsonFile(final String path) {
        try {
            final Reader inFile = new BufferedReader(new FileReader(path));
            try {
                
                @SuppressWarnings("unchecked")
                Map<String, Object> conf = (Map<String, Object>) new JSONParser().parse(inFile, 
                                                                                        containers);
                return new Configuration((String)conf.get("hbase"), 
                                         (String)conf.get("redis"), 
                                         (String)conf.get("amqp"), 
                                         conf);
               
            } finally {
                inFile.close();
            }
        }
        catch (Exception error) {
            return Assert.unreachable(Configuration.class, error);
        }
    }
    
    public String tableName(String name) {
        return prefix_ + name;
    }
    
    private final String hbase_;
    private final String redis_;
    private final String amqp_;
    
    private final String prefix_;
    
    
    @SuppressWarnings("rawtypes")
    private static final ContainerFactory containers = new ContainerFactory() {             
        public List creatArrayContainer() { return new java.util.ArrayList(); }
        public Map createObjectContainer() { return new java.util.HashMap(); }
    };
    
    private Configuration(final String hbase,
                          final String redis,
                          final String amqp,
                          final Map<String, Object> more) {
        hbase_ = hbase;
        redis_ = redis;
        amqp_ = amqp;
        
        prefix_ = more.containsKey("prefix") ? (String)more.get("prefix") : DEFAULT_PREFIX;
        
        // fail fast
        Assert.nonNull(hbase_, redis_, amqp_, prefix_);
    }

}
