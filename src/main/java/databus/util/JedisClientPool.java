package databus.util;

import java.util.Properties;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Created by Qu Chunhe on 2019-09-27.
 */
public class JedisClientPool extends CommonObjectPool<RedisClient> {
    public JedisClientPool(PooledObjectFactory<RedisClient> factory,
                           GenericObjectPoolConfig<RedisClient> config) {
        super(factory, config);
    }

    public static JedisClientPool create(String configFile) {
        Properties properties = Helper.loadProperties(configFile);
        String host = properties.getProperty("host", "127.0.0.1");
        int port = Integer.parseInt(properties.getProperty("port", "6379"));
        int connectionTimeout = Integer.parseInt(properties.getProperty("connectionTimeout","60"));
        int soTimeout = Integer.parseInt(properties.getProperty("soTimeout","60"));
        boolean ssl = Boolean.parseBoolean(properties.getProperty("ssl","false"));
        String password = properties.getProperty("password");
        int database = Integer.parseInt(properties.getProperty("database","0"));
        String clientName = properties.getProperty("clientName");

        int maxTotal = Integer.parseInt(properties.getProperty("maxTotal","5"));
        int maxIdle = Integer.parseInt(properties.getProperty("maxIdle","3"));
        int minIdle = Integer.parseInt(properties.getProperty("minIdle","0"));

        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(minIdle);
        return new JedisClientPool(new PooledObjectFactory<RedisClient>() {
            @Override
            public PooledObject<RedisClient> makeObject() throws Exception {
                final Jedis jedis = new Jedis(host, port, connectionTimeout, soTimeout, ssl);

                try {
                    jedis.connect();
                    if (password != null) {
                        jedis.auth(password);
                    }
                    if (database != 0) {
                        jedis.select(database);
                    }
                    if (clientName != null) {
                        jedis.clientSetname(clientName);
                    }
                } catch (JedisException je) {
                    jedis.close();
                    throw je;
                }
                return  new DefaultPooledObject<>(new JedisClient(jedis));
            }

            @Override
            public void destroyObject(PooledObject<RedisClient> pooledObject) throws Exception {
                Jedis jedis = ((JedisClient) pooledObject.getObject()).getJedis();
                if (jedis.isConnected()) {
                    try {
                        try {
                            jedis.quit();
                        } catch (Exception e) {
                            log.error("can not");
                        }
                        jedis.disconnect();
                    } catch (Exception e) {

                    }
                }
            }

            @Override
            public boolean validateObject(PooledObject<RedisClient> pooledObject) {
                final BinaryJedis jedis = ((JedisClient) pooledObject.getObject()).getJedis();
                try {
                    return  jedis.isConnected() && jedis.ping().equals("PONG");
                } catch (final Exception e) {
                    return false;
                }
            }

            @Override
            public void activateObject(PooledObject<RedisClient> p) throws Exception {

            }

            @Override
            public void passivateObject(PooledObject<RedisClient> p) throws Exception {

            }
        }, config);
    }

    @Override
    public RedisClient getResource() {
        JedisClient jedisClient = (JedisClient) super.getResource();
        jedisClient.setJedisClientPool(this);
        return jedisClient;
    }

    private final static Log log = LogFactory.getLog(JedisClientPool.class);
}
