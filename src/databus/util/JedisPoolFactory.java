package databus.util;

import java.util.Properties;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by Qu Chunhe on 2019-01-08.
 */
public class JedisPoolFactory {

    public static JedisPool create(String configFile) {
        Properties properties = Helper.loadProperties(configFile);
        String host = properties.getProperty("host", "127.0.0.1");
        int port = Integer.parseInt(properties.getProperty("port", "6379"));
        int timeout = Integer.parseInt(properties.getProperty("timeout","60"));
        String password = properties.getProperty("password", "");
        int database = Integer.parseInt(properties.getProperty("database","0"));
        int maxTotal = Integer.parseInt(properties.getProperty("maxTotal","5"));
        int maxIdle = Integer.parseInt(properties.getProperty("maxIdle","3"));
        int minIdle = Integer.parseInt(properties.getProperty("minIdle","0"));

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(minIdle);

        return new JedisPool(config, host, port, timeout, password, database);
    }

}
