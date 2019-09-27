package databus.util;

import redis.clients.jedis.Transaction;
import redis.clients.jedis.commands.JedisCommands;

import java.io.Closeable;

/**
 * Created by Qu Chunhe on 2019-09-24.
 */
public interface RedisClient extends JedisCommands, Closeable {

    Transaction multi();

    boolean doesSupportTransaction();
}
