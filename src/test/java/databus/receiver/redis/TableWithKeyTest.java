package databus.receiver.redis;

import databus.event.mysql.Column;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by Qu Chunhe on 2019-01-25.
 */

@ExtendWith(SpringExtension.class)
@DisplayName("Testing Redis TableWithKey with keys")
@ContextConfiguration(locations = {"file:conf/unit_test.xml"})
public class TableWithKeyTest {

    @Test
    public void testInsertWithOnePrimaryKey() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = system + ":" + name + ":id=3";
            String redisKeyOfKeys = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5";
            jedis.del(redisKey);
            jedis.del(redisKeyOfKeys);

            List<Column> primaryKeys = new LinkedList<>();
            List<Column> row = new LinkedList<>();
            primaryKeys.add(new Column("id", "3", 1));
            row.add(new Column("id", "3", 1));
            row.add(new Column("c3", "5", 1));
            row.add(new Column("c1", "abcde", 1));
            row.add(new Column("c4", null, 1));
            row.add(new Column("c2", "2019-01-22 01:05:07", 1));

            table.insert(jedis, primaryKeys, row);
            assertTrue(jedis.exists(redisKey));

            Map<String, String> value = jedis.hgetAll(redisKey);
            assertEquals(value.get("id"), "3");
            assertEquals(value.get("c1"), "abcde");
            assertEquals(value.get("c2"), "2019-01-22 01:05:07");
            assertEquals(value.get("c3"), "5");
            assertNull(value.get("c4"));

            Set<String> result = jedis.smembers(redisKeyOfKeys);
            assertEquals(result.size(), 1);
            assertTrue(result.contains("id=3"));

            jedis.del(redisKey);
            jedis.del(redisKeyOfKeys);
        } catch (Exception e) {
            log.error("Meet errors", e);
            throw e;
        }
    }

    @Test
    public void testInsertWithThreePrimaryKeys() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = system + ":" + name + ":id1=school&class&id2=2019-01-22&id3=3";
            String redisKeyOfKeys = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5";
            jedis.del(redisKey);
            jedis.del(redisKeyOfKeys);

            List<Column> primaryKeys = new LinkedList<>();
            List<Column> row = new LinkedList<>();
            primaryKeys.add(new Column("id3", "3", 1));
            primaryKeys.add(new Column("id1", "school&class", 1));
            primaryKeys.add(new Column("id2", "2019-01-22", 1));
            row.add(new Column("id3", "3", 1));
            row.add(new Column("id1", "school&class", 1));
            row.add(new Column("id2", "2019-01-22", 1));
            row.add(new Column("c3", "5", 1));
            row.add(new Column("c1", "abcde", 1));
            row.add(new Column("c4", null, 1));
            row.add(new Column("c2", "2019-01-22 01:05:07", 1));

            table.insert(jedis, primaryKeys, row);
            assertTrue(jedis.exists(redisKey));

            Map<String, String> value = jedis.hgetAll(redisKey);
            assertEquals(value.get("id3"), "3");
            assertEquals(value.get("id1"), "school&class");
            assertEquals(value.get("id2"), "2019-01-22");
            assertEquals(value.get("c1"), "abcde");
            assertEquals(value.get("c2"), "2019-01-22 01:05:07");
            assertEquals(value.get("c3"), "5");
            assertNull(value.get("c4"));

            Set<String> result = jedis.smembers(redisKeyOfKeys);
            assertEquals(result.size(), 1);
            assertTrue(result.contains("id1=school&class<&>id2=2019-01-22<&>id3=3"));

            jedis.del(redisKey);
            jedis.del(redisKeyOfKeys);
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testDeleteWithOnePrimaryKey() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = system + ":" + name + ":id=3";
            String redisKeyOfKeys = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5";
            jedis.del(redisKey);
            jedis.del(redisKeyOfKeys);

            List<Column> primaryKeys = new LinkedList<>();
            List<Column> row = new LinkedList<>();
            primaryKeys.add(new Column("id", "3", 1));
            row.add(new Column("id", "3", 1));
            row.add(new Column("c3", "5", 1));
            row.add(new Column("c1", "abcde", 1));
            row.add(new Column("c4", null, 1));
            row.add(new Column("c2", "2019-01-22 01:05:07", 1));

            Map<String, String> value = new HashMap();
            value.put("id", "3");
            value.put("c3", "5");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            jedis.hset(redisKey, value);
            jedis.sadd(redisKeyOfKeys, "id=3");
            table.delete(jedis, primaryKeys, row);
            assertFalse(jedis.exists(redisKey));
            assertFalse(jedis.exists(redisKeyOfKeys));

        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testDeleteWithThreePrimaryKeys() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = system + ":" + name + ":id1=school&class&id2=2019-01-22&id3=3";
            String redisKeyOfKeys = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5";
            jedis.del(redisKey);
            jedis.del(redisKeyOfKeys);

            List<Column> primaryKeys = new LinkedList<>();
            List<Column> row = new LinkedList<>();
            primaryKeys.add(new Column("id3", "3", 1));
            primaryKeys.add(new Column("id1", "school&class", 1));
            primaryKeys.add(new Column("id2", "2019-01-22", 1));
            row.add(new Column("id3", "3", 1));
            row.add(new Column("id1", "school&class", 1));
            row.add(new Column("id2", "2019-01-22", 1));
            row.add(new Column("c3", "5", 1));
            row.add(new Column("c1", "abcde", 1));
            row.add(new Column("c4", null, 1));
            row.add(new Column("c2", "2019-01-22 01:05:07", 1));

            Map<String, String> value = new HashMap();
            value.put("id", "3");
            value.put("c3", "5");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            jedis.hset(redisKey, value);
            jedis.sadd(redisKeyOfKeys, "id1=school&class<&>id2=2019-01-22<&>id3=3");
            table.delete(jedis, primaryKeys, row);
            assertFalse(jedis.exists(redisKey));
            assertFalse(jedis.exists(redisKeyOfKeys));
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testUpdateWithOnePrimaryKey() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = system + ":" + name + ":id=3";
            String redisKeyOfKeysBeforeUpdate = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5";
            String redisKeyOfKeysAfterUpdate = system + ":" + name + ":c3=5&c4=0.13";
            jedis.del(redisKey);
            jedis.del(redisKeyOfKeysBeforeUpdate);
            jedis.del(redisKeyOfKeysAfterUpdate);

            Map<String, String> value = new HashMap();
            value.put("id", "3");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            value.put("c3", "5");
            value.put("c5", "true");
            jedis.hset(redisKey, value);
            jedis.sadd(redisKeyOfKeysBeforeUpdate, "id=3");

            assertTrue(jedis.exists(redisKey));
            Map<String, String> valueBeforeUpdate = jedis.hgetAll(redisKey);
            assertEquals(valueBeforeUpdate.get("id"), "3");
            assertEquals(valueBeforeUpdate.get("c1"), "abcde");
            assertEquals(valueBeforeUpdate.get("c2"), "2019-01-22 01:05:07");
            assertEquals(valueBeforeUpdate.get("c3"), "5");
            assertNull(valueBeforeUpdate.get("c4"));
            assertEquals(valueBeforeUpdate.get("c5"), "true");


            List<Column> primaryKeys = new LinkedList<>();
            List<Column> row = new LinkedList<>();
            primaryKeys.add(new Column("id", "3", 1));
            row.add(new Column("c1", "123456", 1));
            row.add(new Column("c2", null, 1));
            row.add(new Column("c4", "0.13", 1));

            table.update(jedis, primaryKeys, row);

            Map<String, String> valueAfterUpdate = jedis.hgetAll(redisKey);
            assertEquals(valueAfterUpdate.get("id"), "3");
            assertEquals(valueAfterUpdate.get("c1"), "123456");
            assertNull(valueAfterUpdate.get("c2"));
            assertEquals(valueAfterUpdate.get("c3"), "5");
            assertEquals(valueAfterUpdate.get("c4"), "0.13");
            assertEquals(valueAfterUpdate.get("c5"), "true");

            assertFalse(jedis.exists(redisKeyOfKeysBeforeUpdate)&&(jedis.scard(redisKeyOfKeysBeforeUpdate)>1));
            Set<String> result = jedis.smembers(redisKeyOfKeysAfterUpdate);
            assertEquals(result.size(), 1);
            assertTrue(result.contains("id=3"));

            jedis.del(redisKey);
            jedis.del(redisKeyOfKeysBeforeUpdate);
            jedis.del(redisKeyOfKeysAfterUpdate);
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testReplaceWithOnePrimaryKey() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKeyBeforeUpdate = system + ":" + name + ":id=3";
            String redisKeyAfterUpdate = system + ":" + name + ":id=10";
            String redisKeyOfKeysBeforeUpdate = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5";
            String redisKeyOfKeysAfterUpdate = system + ":" + name + ":c3=5&c4=0.13";
            jedis.del(redisKeyBeforeUpdate);
            jedis.del(redisKeyAfterUpdate);
            jedis.del(redisKeyOfKeysBeforeUpdate);
            jedis.del(redisKeyOfKeysAfterUpdate);

            Map<String, String> value = new HashMap();
            value.put("id", "3");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            value.put("c3", "5");
            value.put("c5", "true");
            jedis.hset(redisKeyBeforeUpdate, value);
            jedis.sadd(redisKeyOfKeysBeforeUpdate, "id=3");

            assertTrue(jedis.exists(redisKeyBeforeUpdate));
            Map<String, String> valueBeforeUpdate = jedis.hgetAll(redisKeyBeforeUpdate);
            assertEquals(valueBeforeUpdate.get("id"), "3");
            assertEquals(valueBeforeUpdate.get("c1"), "abcde");
            assertEquals(valueBeforeUpdate.get("c2"), "2019-01-22 01:05:07");
            assertEquals(valueBeforeUpdate.get("c3"), "5");
            assertNull(valueBeforeUpdate.get("c4"));
            assertEquals(valueBeforeUpdate.get("c5"), "true");

            List<Column> primaryKeys = new LinkedList<>();
            List<Column> row = new LinkedList<>();
            primaryKeys.add(new Column("id", "3", 1));
            row.add(new Column("id", "10", 1));
            row.add(new Column("c1", "123456", 1));
            row.add(new Column("c2", null, 1));
            row.add(new Column("c4", "0.13", 1));

            table.update(jedis, primaryKeys, row);

            assertFalse(jedis.exists(redisKeyBeforeUpdate));
            assertTrue(jedis.exists(redisKeyAfterUpdate));
            Map<String, String> valueAfterUpdate = jedis.hgetAll(redisKeyAfterUpdate);
            assertEquals(valueAfterUpdate.get("id"), "10");
            assertEquals(valueAfterUpdate.get("c1"), "123456");
            assertNull(valueAfterUpdate.get("c2"));
            assertEquals(valueAfterUpdate.get("c3"), "5");
            assertEquals(valueAfterUpdate.get("c4"), "0.13");
            assertEquals(valueAfterUpdate.get("c5"), "true");

            assertFalse(jedis.exists(redisKeyOfKeysBeforeUpdate)&&(jedis.scard(redisKeyOfKeysBeforeUpdate)>1));
            Set<String> result = jedis.smembers(redisKeyOfKeysAfterUpdate);
            assertEquals(result.size(), 1);
            assertTrue(result.contains("id=10"));

            jedis.del(redisKeyBeforeUpdate);
            jedis.del(redisKeyAfterUpdate);
            jedis.del(redisKeyOfKeysBeforeUpdate);
            jedis.del(redisKeyOfKeysAfterUpdate);
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }


    @Test
    public void testReplaceWithThreePrimaryKeys() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKeyBeforeUpdate = system + ":" + name + ":id1=3&id2=test";
            String redisKeyAfterUpdate = system + ":" + name + ":id1=3&id3=ok";
            String redisKeyOfKeysBeforeUpdate = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5";
            String redisKeyOfKeysAfterUpdate = system + ":" + name + ":c3=5&c4=0.13";
            jedis.del(redisKeyBeforeUpdate);
            jedis.del(redisKeyAfterUpdate);
            jedis.del(redisKeyOfKeysBeforeUpdate);
            jedis.del(redisKeyOfKeysAfterUpdate);

            Map<String, String> value = new HashMap();
            value.put("id1", "3");
            value.put("id2", "test");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            value.put("c3", "5");
            value.put("c5", "true");
            jedis.hset(redisKeyBeforeUpdate, value);
            jedis.sadd(redisKeyOfKeysBeforeUpdate, "id1=3<&>id2=test");


            assertTrue(jedis.exists(redisKeyBeforeUpdate));
            Map<String, String> valueBeforeUpdate = jedis.hgetAll(redisKeyBeforeUpdate);
            assertEquals(valueBeforeUpdate.get("id1"), "3");
            assertEquals(valueBeforeUpdate.get("id2"), "test");
            assertNull(valueBeforeUpdate.get("id3"));
            assertEquals(valueBeforeUpdate.get("c1"), "abcde");
            assertEquals(valueBeforeUpdate.get("c2"), "2019-01-22 01:05:07");
            assertEquals(valueBeforeUpdate.get("c3"), "5");
            assertNull(valueBeforeUpdate.get("c4"));
            assertEquals(valueBeforeUpdate.get("c5"), "true");


            List<Column> primaryKeys = new LinkedList<>();
            List<Column> row = new LinkedList<>();
            primaryKeys.add(new Column("id1", "3", 1));
            primaryKeys.add(new Column("id2", "test", 1));
            primaryKeys.add(new Column("id3", null, 1));
            row.add(new Column("id1", "3", 1));
            row.add(new Column("id2", null, 1));
            row.add(new Column("id3", "ok", 1));
            row.add(new Column("c1", "123456", 1));
            row.add(new Column("c2", null, 1));
            row.add(new Column("c4", "0.13", 1));

            table.update(jedis, primaryKeys, row);

            assertFalse(jedis.exists(redisKeyBeforeUpdate));
            Map<String, String> valueAfterUpdate = jedis.hgetAll(redisKeyAfterUpdate);
            assertEquals(valueAfterUpdate.get("id1"), "3");
            assertNull(valueAfterUpdate.get("id2"));
            assertEquals(valueAfterUpdate.get("id3"), "ok");
            assertEquals(valueAfterUpdate.get("c1"), "123456");
            assertNull(valueAfterUpdate.get("c2"));
            assertEquals(valueAfterUpdate.get("c3"), "5");
            assertEquals(valueAfterUpdate.get("c4"), "0.13");
            assertEquals(valueAfterUpdate.get("c5"), "true");

            assertFalse(jedis.exists(redisKeyOfKeysBeforeUpdate)&&(jedis.scard(redisKeyOfKeysBeforeUpdate)>1));
            Set<String> result = jedis.smembers(redisKeyOfKeysAfterUpdate);
            assertEquals(result.size(), 1);
            assertTrue(result.contains("id1=3<&>id3=ok"));

            jedis.del(redisKeyBeforeUpdate);
            jedis.del(redisKeyAfterUpdate);
            jedis.del(redisKeyOfKeysBeforeUpdate);
            jedis.del(redisKeyOfKeysAfterUpdate);
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testUpdateWithThreePrimaryKeys() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = system + ":" + name + ":id1=3&id2=test";
            String redisKeyOfKeysBeforeUpdate = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5";
            String redisKeyOfKeysAfterUpdate = system + ":" + name + ":c3=5&c4=0.13";
            jedis.del(redisKey);
            jedis.del(redisKeyOfKeysBeforeUpdate);
            jedis.del(redisKeyOfKeysAfterUpdate);

            Map<String, String> value = new HashMap();
            value.put("id1", "3");
            value.put("id2", "test");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            value.put("c3", "5");
            value.put("c5", "true");
            jedis.hset(redisKey, value);

            assertTrue(jedis.exists(redisKey));
            Map<String, String> valueBeforeUpdate = jedis.hgetAll(redisKey);
            assertEquals(valueBeforeUpdate.get("id1"), "3");
            assertEquals(valueBeforeUpdate.get("id2"), "test");
            assertNull(valueBeforeUpdate.get("id3"));
            assertEquals(valueBeforeUpdate.get("c1"), "abcde");
            assertEquals(valueBeforeUpdate.get("c2"), "2019-01-22 01:05:07");
            assertEquals(valueBeforeUpdate.get("c3"), "5");
            assertNull(valueBeforeUpdate.get("c4"));
            assertEquals(valueBeforeUpdate.get("c5"), "true");


            List<Column> primaryKeys = new LinkedList<>();
            List<Column> row = new LinkedList<>();
            primaryKeys.add(new Column("id1", "3", 1));
            primaryKeys.add(new Column("id2", "test", 1));
            primaryKeys.add(new Column("id3", null, 1));
            row.add(new Column("c1", "123456", 1));
            row.add(new Column("c2", null, 1));
            row.add(new Column("c4", "0.13", 1));

            table.update(jedis, primaryKeys, row);
            Map<String, String> valueAfterUpdate = jedis.hgetAll(redisKey);
            assertEquals(valueAfterUpdate.get("id1"), "3");
            assertEquals(valueBeforeUpdate.get("id2"), "test");
            assertNull(valueBeforeUpdate.get("id3"));
            assertEquals(valueAfterUpdate.get("c1"), "123456");
            assertNull(valueAfterUpdate.get("c2"));
            assertEquals(valueAfterUpdate.get("c3"), "5");
            assertEquals(valueAfterUpdate.get("c4"), "0.13");
            assertEquals(valueAfterUpdate.get("c5"), "true");

            assertFalse(jedis.exists(redisKeyOfKeysBeforeUpdate)&&(jedis.scard(redisKeyOfKeysBeforeUpdate)>1));
            Set<String> result = jedis.smembers(redisKeyOfKeysAfterUpdate);
            assertEquals(result.size(), 1);
            assertTrue(result.contains("id1=3<&>id2=test"));

            jedis.del(redisKey);
            jedis.del(redisKeyOfKeysBeforeUpdate);
            jedis.del(redisKeyOfKeysAfterUpdate);
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    private final static Log log = LogFactory.getLog(TableWithKeyTest.class);

    @Autowired
    private JedisPool jedisPool;

    private String system = "unit_test";
    private String name = "test";
}
