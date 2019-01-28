package databus.receiver.redis;

import databus.event.mysql.Column;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by Qu Chunhe on 2019-01-18.
 */

@ExtendWith(SpringExtension.class)
@DisplayName("Testing Redis Table")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ContextConfiguration(locations = {"file:conf/unit_test.xml"})
class TableTest {

    @BeforeAll
    public void ini() {
        table = new Table(system, name);
    }

    @Test
    public void testInsertWithOnePrimaryKey() {
        try(Jedis jedis = jedisPool.getResource()) {
            List<Column> primaryKeys = new LinkedList<>();
            List<Column> row = new LinkedList<>();
            primaryKeys.add(new Column("id", "3", 1));
            row.add(new Column("id", "3", 1));
            row.add(new Column("c3", "5", 1));
            row.add(new Column("c1", "abcde", 1));
            row.add(new Column("c4", null, 1));
            row.add(new Column("c2", "2019-01-22 01:05:07", 1));
            String key = system+":"+name+":id=3";
            jedis.del(key);

            table.insert(jedis, primaryKeys, row);
            assertTrue(jedis.exists(key));

            Map<String, String> value = jedis.hgetAll(key);
            assertEquals(value.get("id"), "3");
            assertEquals(value.get("c1"), "abcde");
            assertEquals(value.get("c2"), "2019-01-22 01:05:07");
            assertEquals(value.get("c3"), "5");
            assertNull(value.get("c4"));
            jedis.del(key);
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testInsertWithThreePrimaryKeys() {
        try(Jedis jedis = jedisPool.getResource()) {
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
            String key = system+":"+name+":id1=school&class&id2=2019-01-22&id3=3";
            jedis.del(key);

            table.insert(jedis, primaryKeys, row);
            assertTrue(jedis.exists(key));

            Map<String, String> value = jedis.hgetAll(key);
            assertEquals(value.get("id3"), "3");
            assertEquals(value.get("id1"), "school&class");
            assertEquals(value.get("id2"), "2019-01-22");
            assertEquals(value.get("c1"), "abcde");
            assertEquals(value.get("c2"), "2019-01-22 01:05:07");
            assertEquals(value.get("c3"), "5");
            assertNull(value.get("c4"));
            jedis.del(key);
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testDeleteWithOnePrimaryKey() {
            try(Jedis jedis = jedisPool.getResource()) {
            List<Column> primaryKeys = new LinkedList<>();
            List<Column> row = new LinkedList<>();
            primaryKeys.add(new Column("id", "3", 1));
            row.add(new Column("id", "3", 1));
            row.add(new Column("c3", "5", 1));
            row.add(new Column("c1", "abcde", 1));
            row.add(new Column("c4", null, 1));
            row.add(new Column("c2", "2019-01-22 01:05:07", 1));
            String key = system+":"+name+":id=3";
            jedis.del(key);
            Map<String, String> value = new HashMap();
            value.put("id", "3");
            value.put("c3", "5");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            jedis.hset(key, value);
            table.delete(jedis, primaryKeys, row);
            assertFalse(jedis.exists(key));
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testDeleteWithThreePrimaryKeys() {
        try(Jedis jedis = jedisPool.getResource()) {
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
            String key = system+":"+name+":id1=school&class&id2=2019-01-22&id3=3";
            jedis.del(key);
            Map<String, String> value = new HashMap();
            value.put("id", "3");
            value.put("c3", "5");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            jedis.hset(key, value);
            table.delete(jedis, primaryKeys, row);
            assertFalse(jedis.exists(key));
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testUpdateWithOnePrimaryKey() {
        try(Jedis jedis = jedisPool.getResource()) {
            String key = system+":"+name+":id=3";
            jedis.del(key);

            Map<String, String> value = new HashMap();
            value.put("id", "3");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            value.put("c3", "5");
            value.put("c5", "true");
            jedis.hset(key, value);

            assertTrue(jedis.exists(key));
            Map<String, String> valueBeforeUpdate = jedis.hgetAll(key);
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

            Map<String, String> valueAfterUpdate = jedis.hgetAll(key);
            assertEquals(valueAfterUpdate.get("id"), "3");
            assertEquals(valueAfterUpdate.get("c1"), "123456");
            assertNull(valueAfterUpdate.get("c2"));
            assertEquals(valueAfterUpdate.get("c3"), "5");
            assertEquals(valueAfterUpdate.get("c4"), "0.13");
            assertEquals(valueAfterUpdate.get("c5"), "true");

            jedis.del(key);
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testReplaceWithOnePrimaryKey() {
        try(Jedis jedis = jedisPool.getResource()) {
            String keyBeforeUpdate = system+":"+name+":id=3";
            String keyAfterUpdate = system+":"+name+":id=10";
            jedis.del(keyBeforeUpdate);
            jedis.del(keyAfterUpdate);

            Map<String, String> value = new HashMap();
            value.put("id", "3");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            value.put("c3", "5");
            value.put("c5", "true");
            jedis.hset(keyBeforeUpdate, value);

            assertTrue(jedis.exists(keyBeforeUpdate));
            Map<String, String> valueBeforeUpdate = jedis.hgetAll(keyBeforeUpdate);
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

            assertFalse(jedis.exists(keyBeforeUpdate));
            assertTrue(jedis.exists(keyAfterUpdate));
            Map<String, String> valueAfterUpdate = jedis.hgetAll(keyAfterUpdate);
            assertEquals(valueAfterUpdate.get("id"), "10");
            assertEquals(valueAfterUpdate.get("c1"), "123456");
            assertNull(valueAfterUpdate.get("c2"));
            assertEquals(valueAfterUpdate.get("c3"), "5");
            assertEquals(valueAfterUpdate.get("c4"), "0.13");
            assertEquals(valueAfterUpdate.get("c5"), "true");

            jedis.del(keyBeforeUpdate);
            jedis.del(keyAfterUpdate);
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testReplaceWithThreePrimaryKeys() {
        try(Jedis jedis = jedisPool.getResource()) {
            String keyBeforeUpdate = system+":"+name+":id1=3&id2=test";
            String keyAfterUpdate = system+":"+name+":id1=3&id3=ok";
            jedis.del(keyBeforeUpdate);
            jedis.del(keyAfterUpdate);

            Map<String, String> value = new HashMap();
            value.put("id1", "3");
            value.put("id2", "test");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            value.put("c3", "5");
            value.put("c5", "true");
            jedis.hset(keyBeforeUpdate, value);

            assertTrue(jedis.exists(keyBeforeUpdate));
            Map<String, String> valueBeforeUpdate = jedis.hgetAll(keyBeforeUpdate);
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

            assertFalse(jedis.exists(keyBeforeUpdate));
            Map<String, String> valueAfterUpdate = jedis.hgetAll(keyAfterUpdate);
            assertEquals(valueAfterUpdate.get("id1"), "3");
            assertNull(valueAfterUpdate.get("id2"));
            assertEquals(valueAfterUpdate.get("id3"), "ok");
            assertEquals(valueAfterUpdate.get("c1"), "123456");
            assertNull(valueAfterUpdate.get("c2"));
            assertEquals(valueAfterUpdate.get("c3"), "5");
            assertEquals(valueAfterUpdate.get("c4"), "0.13");
            assertEquals(valueAfterUpdate.get("c5"), "true");

            jedis.del(keyBeforeUpdate);
            jedis.del(keyAfterUpdate);
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Test
    public void testUpdateWithThreePrimaryKeys() {
        try(Jedis jedis = jedisPool.getResource()) {
            String key = system+":"+name+":id1=3&id2=test";
            jedis.del(key);


            Map<String, String> value = new HashMap();
            value.put("id1", "3");
            value.put("id2", "test");
            value.put("c1", "abcde");
            value.put("c2", "2019-01-22 01:05:07");
            value.put("c3", "5");
            value.put("c5", "true");
            jedis.hset(key, value);

            assertTrue(jedis.exists(key));
            Map<String, String> valueBeforeUpdate = jedis.hgetAll(key);
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
            Map<String, String> valueAfterUpdate = jedis.hgetAll(key);
            assertEquals(valueAfterUpdate.get("id1"), "3");
            assertEquals(valueBeforeUpdate.get("id2"), "test");
            assertNull(valueBeforeUpdate.get("id3"));
            assertEquals(valueAfterUpdate.get("c1"), "123456");
            assertNull(valueAfterUpdate.get("c2"));
            assertEquals(valueAfterUpdate.get("c3"), "5");
            assertEquals(valueAfterUpdate.get("c4"), "0.13");
            assertEquals(valueAfterUpdate.get("c5"), "true");

            jedis.del(key);
        } catch (Exception e) {
            log.error("Meet errors", e);
        }
    }

    @Autowired
    protected JedisPool jedisPool;

    protected String system = "unit_test";
    protected String name = "test";
    protected Table table;

    private final static Log log = LogFactory.getLog(TableTest.class);
}