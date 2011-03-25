package org.mozilla.grouper.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

public class Config {

    // :TODO:
    // Knowing all possible keys here is not very maintainable (bad cohesion) but good to find
    // config errors fast.
    // A better approach would register modules with the configuration and delegate the
    // initialization (later) of the respective properties to those modules. Or we use spring.
    // Also we might want to have a generic mechanism to specify hbase/redis/rabbitmq client props.
    public String keyScheme() { return keyScheme_; }
    public String prefix() { return prefix_; }
    public String dfsBase() { return dfsBase_; }
    public String redis() { return redis_; }
    public String amqp() { return rabbitmq_; }
    public String hbaseZk() { return hbaseZk_; }
    public String hbaseZkNode() { return hbaseZkNode_; }

    /** Convert config to a map, e.g. for serialization. */
    public Map<String, String> asMap() {
        if (map_ != null) return map_;
        Map<String, String> map = new HashMap<String, String>();
        map.put("redis",       redis_);
        map.put("rabbitmq",    rabbitmq_);
        map.put("prefix",      prefix_);
        map.put("keyScheme",   keyScheme_);
        map.put("dfsBase",  dfsBase_);
        if (hbaseZk_ != null) map.put("hbaseZk", hbaseZk_);
        if (hbaseZkNode_ != null) map.put("hbaseZkNode", hbaseZkNode_);

        // Litmus test: fail now rather than on restore:
        new Config(map);

        map_ = map;
        return map_;
    }

    /** Reconstruct config from map */
    public Config(Map<String, ? extends Object> map) {
        redis_       = require("redis", map, null);
        rabbitmq_    = require("rabbitmq", map, null);
        prefix_      = require("prefix", map, null);
        keyScheme_   = require("keyScheme", map, null);
        dfsBase_  = require("dfsBase", map, null);
        hbaseZk_     = get("hbaseZk", map, null);
        hbaseZkNode_ = get("hbaseZkNode", map, null);
    }

    /**
     * Returns configuration for the given path, or $GROUPERFISH_HOME/conf/grouperfish.json
     * if the path is null.
     * Defaults are instantiated from <tt>$GROUPERFISH_HOME/conf/defaults.json</tt>
     * If <tt>$GROUPERFISH_HOME</tt> is not defined by the environment, use <tt>$(pwd)/../../</tt>
     *
     * @param String Path to user-specified configuration (JSON file). May be <tt>null</tt>.
     */
    public Config(String configPath) {
        final char S = File.separatorChar;
        String home = null;
        try { home = System.getenv("GROUPERFISH_HOME"); }
        catch (NullPointerException npe) { /* nom nom */  }
        if (home == null) {
            String p = new StringBuilder("..").append(S).append("..").append(S).toString();
            home = new File(p).getAbsolutePath();
        }

        // Load defaults (if available)
        final String defaultsPath = new StringBuilder(home).append(S).append("conf").append(S)
                                    .append("defaults.json").toString();
        final Map<String, Object> defaults = map(defaultsPath);

        // Load user config
        if (configPath == null) {
            configPath = new StringBuilder(home).append(S).append("conf").append(S)
                         .append("grouperfish.json").toString();
        }
        final Map<String, Object> values = map(configPath);
        if (values == null && defaults == null) {
            // Fail fast for missing config:
            throw new RuntimeException("Could not load user configuration nor defaults.");
        }

        // RAII. Fail fast for missing / invalid config values:
        redis_      = require("redis",     values, defaults);
        rabbitmq_   = require("rabbitmq",  values, defaults);
        prefix_     = require("prefix",    values, defaults);
        keyScheme_  = require("keyScheme", values, defaults);
        dfsBase_ = require("dfsBase", values, defaults);

        hbaseZk_     = get("hbaseZk",     values, defaults);
        hbaseZkNode_ = get("hbaseZkNode", values, defaults);
    }

    @SuppressWarnings("rawtypes")
    private static final ContainerFactory containers = new ContainerFactory() {
        @Override
        public List creatArrayContainer() { return new java.util.ArrayList(); }
        @Override
        public Map createObjectContainer() { return new java.util.HashMap(); }
    };

    @SuppressWarnings("unchecked")
    private Map<String, Object> map(final String path) {
        if (!new File(path).exists()) return null;
        try {
            final Reader inFile = new BufferedReader(new FileReader(path));
            try {
                return (Map<String, Object>) new JSONParser().parse(inFile, containers);

            } finally {
                inFile.close();
            }
        }
        catch (Exception error) {
            throw new RuntimeException(error);
        }
    }

    private String get(String key, Map<String, ? extends Object> map, Map<String, Object> defaults) {
        if (map != null && map.containsKey(key) && map.get(key) != null)
            return map.get(key).toString();
        if (defaults != null && defaults.containsKey(key) && defaults.get(key) != null)
            return defaults.get(key).toString();
        return null;
    }

    private String require(String key, Map<String, ? extends Object> map, Map<String, Object> defaults) {
        String value = get(key, map, defaults);
        if (value != null) return value;
        return Assert.unreachable(String.class, "Missing configuration for key: %s", key);
    }

    private final String redis_;
    private final String rabbitmq_;
    private final String keyScheme_;
    private final String prefix_;
    private final String dfsBase_;
    private final String hbaseZk_;
    private final String hbaseZkNode_;

    private Map<String, String> map_;
}
