package databus.receiver.redis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import redis.clients.jedis.Jedis;

import java.util.LinkedList;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by Qu Chunhe on 2019-01-25.
 */

@ExtendWith(SpringExtension.class)
@DisplayName("Testing Redis TableWithKey without key")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ContextConfiguration(locations = {"file:conf/unit_test.xml"})
public class TableWithKeyTestWhenEmptyKeys extends TableTest {

    @Override
    @BeforeAll
    public void ini() {
        table = new TableWithKey(system, name);
        ((TableWithKey)table).setKeys(new LinkedList<>());
    }

    @Override
    @Test
    public void testInsertWithOnePrimaryKey() {
        String redisKeyOfKeys = system+":"+name+":";
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.del(redisKeyOfKeys);
        }
        super.testInsertWithOnePrimaryKey();
        try(Jedis jedis = jedisPool.getResource()) {
            Set<String> result = jedis.smembers(redisKeyOfKeys);
            assertTrue(result.size()==1);
            assertTrue(result.contains("id=3"));
            jedis.del(redisKeyOfKeys);
        }
    }

    @Override
    @Test
    public void testInsertWithThreePrimaryKeys() {
        String redisKeyOfKeys = system+":"+name+":";
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.del(redisKeyOfKeys);
        }
        super.testInsertWithThreePrimaryKeys();
        try(Jedis jedis = jedisPool.getResource()) {
            Set<String> result = jedis.smembers(redisKeyOfKeys);
            assertTrue(result.size()==1);
            assertTrue(result.contains("id1=school&class<&>id2=2019-01-22<&>id3=3"));
            jedis.del(redisKeyOfKeys);
        }
    }

    @Override
    @Test
    public void testDeleteWithOnePrimaryKey() {
        String redisKeyOfKeys = system+":"+name+":";
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.del(redisKeyOfKeys);
            jedis.sadd(redisKeyOfKeys, "id=3");
        }
        super.testDeleteWithOnePrimaryKey();
        try(Jedis jedis = jedisPool.getResource()) {
            Set<String> result = jedis.smembers(redisKeyOfKeys);
            assertTrue(result.size()==0);
            jedis.del(redisKeyOfKeys);
        }
    }

    @Override
    @Test
    public void testDeleteWithThreePrimaryKeys() {
        String redisKeyOfKeys = system+":"+name+":";
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.del(redisKeyOfKeys);
            jedis.sadd(redisKeyOfKeys, "id1=school&class<&>id2=2019-01-22<&>id3=3");
        }
        super.testDeleteWithThreePrimaryKeys();
        try(Jedis jedis = jedisPool.getResource()) {
            Set<String> result = jedis.smembers(redisKeyOfKeys);
            assertTrue(result.size()==0);
            jedis.del(redisKeyOfKeys);
        }
    }

    @Override
    @Test
    public void testUpdateWithOnePrimaryKey() {
        String redisKeyOfKeys = system+":"+name+":";
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.del(redisKeyOfKeys);
        }
        super.testUpdateWithOnePrimaryKey();
        try(Jedis jedis = jedisPool.getResource()) {
            assertTrue(!jedis.exists(redisKeyOfKeys) || jedis.scard(redisKeyOfKeys)==0);
            jedis.del(system+":"+name+":");
        }
    }

    @Override
    @Test
    public void testReplaceWithOnePrimaryKey() {
        String redisKeyOfKeys = system+":"+name+":";
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.del(redisKeyOfKeys);
        }
        super.testReplaceWithOnePrimaryKey();
        try(Jedis jedis = jedisPool.getResource()) {
            Set<String> result = jedis.smembers(redisKeyOfKeys);
            assertTrue(result.size()==1);
            assertTrue(result.contains("id=10"));
            jedis.del(redisKeyOfKeys);
        }
    }

    @Override
    @Test
    public void testReplaceWithThreePrimaryKeys() {
        String redisKeyOfKeys = system+":"+name+":";
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.del(redisKeyOfKeys);
        }
        super.testReplaceWithThreePrimaryKeys();
        try(Jedis jedis = jedisPool.getResource()) {
            Set<String> result = jedis.smembers(redisKeyOfKeys);
            assertTrue(result.size()==1);
            assertTrue(result.contains("id1=3<&>id3=ok"));
            jedis.del(redisKeyOfKeys);
        }
    }

    @Override
    @Test
    public void testUpdateWithThreePrimaryKeys() {
        String redisKeyOfKeys = system+":"+name+":";
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.del(redisKeyOfKeys);
        }
        super.testUpdateWithThreePrimaryKeys();
        try(Jedis jedis = jedisPool.getResource()) {
            assertTrue(!jedis.exists(redisKeyOfKeys) || jedis.scard(redisKeyOfKeys)==0);
            jedis.del(system+":"+name+":");
        }
    }
}
