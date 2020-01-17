package databus.util;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Created by Qu Chunhe on 2019-09-27.
 */
public class JedisClusterClientPool implements RedisClientPool {

    public JedisClusterClientPool(JedisClusterClient jedisClusterClient) {
        this.jedisClusterClient = jedisClusterClient;
    }

    public static JedisClusterClientPool create(String configFile) {
        Properties properties = Helper.loadProperties(configFile);

        String nodesValue = properties.getProperty("nodes", "127.0.0.1:6379");
        String[] nodes = nodesValue.split(",");
        Set<HostAndPort> jedisClusterNode = new HashSet<>();
        for(String n : nodes) {
            String[] parts = n.split(":");
            String host = parts[0].trim();
            int port = null==parts[1] ? 6379 : Integer.parseInt(parts[1]);
            jedisClusterNode.add(new HostAndPort(host, port));
        }
        int connectionTimeout = Integer.parseInt(properties.getProperty("connectionTimeout","60"));
        int soTimeout = Integer.parseInt(properties.getProperty("soTimeout","60"));
        String password = properties.getProperty("password");
        int maxAttempts = Integer.parseInt(properties.getProperty("maxAttempts","10"));
        String clientName = properties.getProperty("clientName","JedisClusterClient");

        int maxTotal = Integer.parseInt(properties.getProperty("maxTotal","5"));
        int maxIdle = Integer.parseInt(properties.getProperty("maxIdle","3"));
        int minIdle = Integer.parseInt(properties.getProperty("minIdle","0"));
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);

        return new JedisClusterClientPool(new JedisClusterClient(new JedisCluster(jedisClusterNode,
                                                                                  connectionTimeout,
                                                                                  soTimeout,
                                                                                  maxAttempts,
                                                                                  password,
                                                                                  clientName,
                                                                                  poolConfig)));
    }


    @Override
    public RedisClient getRedisClient() {
        return jedisClusterClient;
    }

    @Override
    public void close() throws IOException {
        jedisClusterClient.close();
    }

    private final JedisClusterClient jedisClusterClient;
}
