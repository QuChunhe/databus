package databus.util;

import databus.event.mysql.Column;
import databus.receiver.redis2.Table;
import databus.receiver.redis2.TableWithKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by Qu Chunhe on 2020-01-15.
 */
@ExtendWith(SpringExtension.class)
@DisplayName("Testing RedisCache4MysqlTest")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ContextConfiguration(locations = {"file:conf/unit_test.xml"})
public class RedisCache4MysqlTest {

    @BeforeAll
    public void ini() {
        redisCache.setSystem(system);
    }

    @AfterAll
    public void destroy() {

    }

    @Test
    public void testGetRowByPrimaryKeys() {
        Table table = new Table(system, name);
        List<Column> primaryKeys = new LinkedList<>();
        primaryKeys.add(new Column("id3", "3", 1));
        primaryKeys.add(new Column("id1", "school&class", 1));
        primaryKeys.add(new Column("id2", "2019-01-22", 1));

        List<Column> row = new LinkedList<>();
        row.add(new Column("id3", "3", 1));
        row.add(new Column("id1", "school&class", 1));
        row.add(new Column("id2", "2019-01-22", 1));
        row.add(new Column("c3", "5", 1));
        row.add(new Column("c1", "abcde", 1));
        row.add(new Column("c4", null, 1));
        row.add(new Column("c2", "2019-01-22 01:05:07", 1));

        String key = system + ":" + name + ":id1=school&class&id2=2019-01-22&id3=3";
        redisClient.del(key);

        table.insert(redisClient, primaryKeys, row);

        HashMap<String, String> pk = new HashMap<>();
        pk.put("id3", "3");
        pk.put("id1", "school&class");
        pk.put("id2", "2019-01-22");
        Map<String, String>  value = redisCache.getRowByPrimaryKeys(name, pk);

        assertEquals(value.get("id3"), "3");
        assertEquals(value.get("id1"), "school&class");
        assertEquals(value.get("id2"), "2019-01-22");
        assertEquals(value.get("c1"), "abcde");
        assertEquals(value.get("c2"), "2019-01-22 01:05:07");
        assertEquals(value.get("c3"), "5");
        assertNull(value.get("c4"));

        redisClient.del(key);
    }

    @Test
    public void testGetRowByKeys() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);

        String redisKey1 = system + ":" + name + ":id=3&id3=test";
        String redisKeyOfKeys1 = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5:index";
        String redisKey2 = system + ":" + name + ":id=1&id3=us";

        redisClient.del(redisKey1);
        redisClient.del(redisKeyOfKeys1);

        List<Column> primaryKeys1 = new LinkedList<>();
        primaryKeys1.add(new Column("id3", "test", 1));
        primaryKeys1.add(new Column("id", "3", 1));

        List<Column> row1 = new LinkedList<>();
        row1.add(new Column("id", "3", 1));
        row1.add(new Column("id3", "test", 1));
        row1.add(new Column("c3", "5", 1));
        row1.add(new Column("c1", "abcde", 1));
        row1.add(new Column("c4", null, 1));
        row1.add(new Column("c2", "2019-01-22 01:05:07", 1));

        table.insert(redisClient, primaryKeys1, row1);

        List<Column> primaryKeys2 = new LinkedList<>();
        primaryKeys2.add(new Column("id3", "us", 1));
        primaryKeys2.add(new Column("id", "1", 1));

        List<Column> row2 = new LinkedList<>();
        row2.add(new Column("id", "1", 1));
        row2.add(new Column("id3", "us", 1));
        row2.add(new Column("c3", "5", 1));
        row2.add(new Column("c1", "12345", 1));
        row2.add(new Column("c4", null, 1));
        row2.add(new Column("c2", "2019-01-22 01:05:07", 1));

        table.insert(redisClient, primaryKeys2, row2);

        HashMap<String, String> k = new HashMap<>();
        k.put("c2", "2019-01-22 01:05:07");
        k.put("c3", "5");
        k.put("c4", null);
        Map<String, String> value = redisCache.getRowByKeys(name, k);

        assertTrue(value.get("id").equals("1") || value.get("id").equals("3"));
        assertTrue(value.get("id3").equals("us") || value.get("id3").equals("test"));
        assertTrue(value.get("c1").equals("12345") || value.get("c1").equals("abcde"));
        assertEquals(value.get("c3"), "5");
        assertEquals(value.get("c2"), "2019-01-22 01:05:07");
        assertNull(value.get("c4"));

        redisClient.del(redisKey1);
        redisClient.del(redisKeyOfKeys1);
        redisClient.del(redisKey2);
    }

    @Test
    public void testGetRowsByKey() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);

        String redisKey1 = system + ":" + name + ":id=3&id3=test";
        String redisKeyOfKeys1 = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5:index";
        String redisKey2 = system + ":" + name + ":id=1&id3=us";

        redisClient.del(redisKey1);
        redisClient.del(redisKeyOfKeys1);

        List<Column> primaryKeys1 = new LinkedList<>();
        primaryKeys1.add(new Column("id3", "test", 1));
        primaryKeys1.add(new Column("id", "3", 1));

        List<Column> row1 = new LinkedList<>();
        row1.add(new Column("id", "3", 1));
        row1.add(new Column("id3", "test", 1));
        row1.add(new Column("c3", "5", 1));
        row1.add(new Column("c1", "abcde", 1));
        row1.add(new Column("c4", null, 1));
        row1.add(new Column("c2", "2019-01-22 01:05:07", 1));

        table.insert(redisClient, primaryKeys1, row1);

        List<Column> primaryKeys2 = new LinkedList<>();
        primaryKeys2.add(new Column("id3", "us", 1));
        primaryKeys2.add(new Column("id", "1", 1));

        List<Column> row2 = new LinkedList<>();
        row2.add(new Column("id", "1", 1));
        row2.add(new Column("id3", "us", 1));
        row2.add(new Column("c3", "5", 1));
        row2.add(new Column("c1", "12345", 1));
        row2.add(new Column("c4", null, 1));
        row2.add(new Column("c2", "2019-01-22 01:05:07", 1));

        table.insert(redisClient, primaryKeys2, row2);

        HashMap<String, String> k = new HashMap<>();
        k.put("c2", "2019-01-22 01:05:07");
        k.put("c3", "5");
        k.put("c4", null);
        Collection<Map<String, String>> values = redisCache.getRowsByKeys(name, k, 2);

        for(Map<String, String> value : values) {
            assertTrue(value.get("id").equals("1") || value.get("id").equals("3"));
            assertTrue(value.get("id3").equals("us") || value.get("id3").equals("test"));
            assertTrue(value.get("c1").equals("12345") || value.get("c1").equals("abcde"));
            assertEquals(value.get("c3"), "5");
            assertEquals(value.get("c2"), "2019-01-22 01:05:07");
            assertNull(value.get("c4"));
        }

        redisClient.del(redisKey1);
        redisClient.del(redisKeyOfKeys1);
        redisClient.del(redisKey2);
    }

    @Test
    public void testGetValueByPrimaryKeys() {
        Table table = new Table(system, name);
        List<Column> primaryKeys = new LinkedList<>();
        primaryKeys.add(new Column("id3", "3", 1));
        primaryKeys.add(new Column("id1", "school&class", 1));
        primaryKeys.add(new Column("id2", "2019-01-22", 1));

        List<Column> row = new LinkedList<>();
        row.add(new Column("id3", "3", 1));
        row.add(new Column("id1", "school&class", 1));
        row.add(new Column("id2", "2019-01-22", 1));
        row.add(new Column("c3", "5", 1));
        row.add(new Column("c1", "abcde", 1));
        row.add(new Column("c4", null, 1));
        row.add(new Column("c2", "2019-01-22 01:05:07", 1));

        String key = system + ":" + name + ":id1=school&class&id2=2019-01-22&id3=3";
        redisClient.del(key);

        table.insert(redisClient, primaryKeys, row);

        HashMap<String, String> pk = new HashMap<>();
        pk.put("id3", "3");
        pk.put("id1", "school&class");
        pk.put("id2", "2019-01-22");

        assertEquals(redisCache.getValueByPrimaryKeys(name, pk, "id3"), "3");
        assertEquals(redisCache.getValueByPrimaryKeys(name, pk, "id1"), "school&class");
        assertEquals(redisCache.getValueByPrimaryKeys(name, pk, "id2"), "2019-01-22");
        assertEquals(redisCache.getValueByPrimaryKeys(name, pk, "c1"), "abcde");
        assertEquals(redisCache.getValueByPrimaryKeys(name, pk, "c2"), "2019-01-22 01:05:07");
        assertEquals(redisCache.getValueByPrimaryKeys(name, pk, "c3"), "5");
        assertNull(redisCache.getValueByPrimaryKeys(name, pk, "c4"));

        redisClient.del(key);
    }

    @Test
    public void testGetValuesByPrimaryKeys() {
        Table table = new Table(system, name);
        List<Column> primaryKeys = new LinkedList<>();
        primaryKeys.add(new Column("id3", "3", 1));
        primaryKeys.add(new Column("id1", "school&class", 1));
        primaryKeys.add(new Column("id2", "2019-01-22", 1));

        List<Column> row = new LinkedList<>();
        row.add(new Column("id3", "3", 1));
        row.add(new Column("id1", "school&class", 1));
        row.add(new Column("id2", "2019-01-22", 1));
        row.add(new Column("c3", "5", 1));
        row.add(new Column("c1", "abcde", 1));
        row.add(new Column("c4", null, 1));
        row.add(new Column("c2", "2019-01-22 01:05:07", 1));

        String key = system + ":" + name + ":id1=school&class&id2=2019-01-22&id3=3";
        redisClient.del(key);

        table.insert(redisClient, primaryKeys, row);

        HashMap<String, String> pk = new HashMap<>();
        pk.put("id3", "3");
        pk.put("id1", "school&class");
        pk.put("id2", "2019-01-22");
        String[] value = redisCache.getValuesByPrimaryKeys(name, pk, new String[]{"id3", "id1","id2", "c1", "c2","c3", "c4"});
        assertEquals(value[0], "3");
        assertEquals(value[1], "school&class");
        assertEquals(value[2], "2019-01-22");
        assertEquals(value[3], "abcde");
        assertEquals(value[4], "2019-01-22 01:05:07");
        assertEquals(value[5], "5");
        assertNull(value[6]);

        redisClient.del(key);
    }

    @Test
    public void testGetValuesByKeys() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);

        String redisKey1 = system + ":" + name + ":id=3&id3=test";
        String redisKeyOfKeys1 = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5";
        String redisKey2 = system + ":" + name + ":id=1&id3=us";

        redisClient.del(redisKey1);
        redisClient.del(redisKeyOfKeys1);

        List<Column> primaryKeys1 = new LinkedList<>();
        primaryKeys1.add(new Column("id3", "test", 1));
        primaryKeys1.add(new Column("id", "3", 1));

        List<Column> row1 = new LinkedList<>();
        row1.add(new Column("id", "3", 1));
        row1.add(new Column("id3", "test", 1));
        row1.add(new Column("c3", "5", 1));
        row1.add(new Column("c1", "abcde", 1));
        row1.add(new Column("c4", null, 1));
        row1.add(new Column("c2", "2019-01-22 01:05:07", 1));

        table.insert(redisClient, primaryKeys1, row1);

        List<Column> primaryKeys2 = new LinkedList<>();
        primaryKeys2.add(new Column("id3", "us", 1));
        primaryKeys2.add(new Column("id", "1", 1));

        List<Column> row2 = new LinkedList<>();
        row2.add(new Column("id", "1", 1));
        row2.add(new Column("id3", "us", 1));
        row2.add(new Column("c3", "5", 1));
        row2.add(new Column("c1", "12345", 1));
        row2.add(new Column("c4", null, 1));
        row2.add(new Column("c2", "2019-01-22 01:05:07", 1));

        table.insert(redisClient, primaryKeys2, row2);

        HashMap<String, String> k = new HashMap<>();
        k.put("c2", "2019-01-22 01:05:07");
        k.put("c3", "5");
        k.put("c4", null);
        Collection<String[]> values = redisCache.getValuesByKeys(name, k, new String[]{"id", "id3", "c1", "c2","c3", "c4"});
        for (String[] value : values) {
            assertTrue(value[0].equals("1") || value[0].equals("3"));
            assertTrue(value[1].equals("us") || value[1].equals("test"));
            assertTrue(value[2].equals("12345") || value[2].equals("abcde"));
            assertEquals(value[3], "2019-01-22 01:05:07");
            assertEquals(value[4], "5");
            assertNull(value[5]);
        }


        redisClient.del(redisKey1);
        redisClient.del(redisKeyOfKeys1);
        redisClient.del(redisKey2);
    }

    @Test
    public void testGetValueByKeys() {
        TableWithKey table = new TableWithKey(system, name);
        List<String> keys = new LinkedList();
        keys.add("c2");
        keys.add("c4");
        keys.add("c3");
        table.setKeys(keys);

        String redisKey1 = system + ":" + name + ":id=3&id3=test";
        String redisKeyOfKeys1 = system + ":" + name + ":c2=2019-01-22 01:05:07&c3=5:index";
        String redisKey2 = system + ":" + name + ":id=1&id3=us";

        redisClient.del(redisKey1);
        redisClient.del(redisKeyOfKeys1);

        List<Column> primaryKeys1 = new LinkedList<>();
        primaryKeys1.add(new Column("id3", "test", 1));
        primaryKeys1.add(new Column("id", "3", 1));

        List<Column> row1 = new LinkedList<>();
        row1.add(new Column("id", "3", 1));
        row1.add(new Column("id3", "test", 1));
        row1.add(new Column("c3", "5", 1));
        row1.add(new Column("c1", "abcde", 1));
        row1.add(new Column("c4", null, 1));
        row1.add(new Column("c2", "2019-01-22 01:05:07", 1));

        table.insert(redisClient, primaryKeys1, row1);

        List<Column> primaryKeys2 = new LinkedList<>();
        primaryKeys2.add(new Column("id3", "us", 1));
        primaryKeys2.add(new Column("id", "1", 1));

        List<Column> row2 = new LinkedList<>();
        row2.add(new Column("id", "1", 1));
        row2.add(new Column("id3", "us", 1));
        row2.add(new Column("c3", "5", 1));
        row2.add(new Column("c1", "12345", 1));
        row2.add(new Column("c4", null, 1));
        row2.add(new Column("c2", "2019-01-22 01:05:07", 1));

        table.insert(redisClient, primaryKeys2, row2);

        HashMap<String, String> k = new HashMap<>();
        k.put("c2", "2019-01-22 01:05:07");
        k.put("c3", "5");
        k.put("c4", null);
        String value;
        value =  redisCache.getValueByKeys(name, k, "id");
        assertTrue(value.equals("3")||value.equals("1"));
        if (value.equals("1")) {
            assertEquals(redisCache.getValueByKeys(name, k, "c1"), "12345");
        } else {
            assertEquals(redisCache.getValueByKeys(name, k, "c1"), "abcde");
        }

        assertNull(redisCache.getValueByKeys(name, k, "c4"));


        redisClient.del(redisKey1);
        redisClient.del(redisKeyOfKeys1);
        redisClient.del(redisKey2);
    }

    @Autowired
    protected RedisCache4Mysql redisCache;
    protected String system = "unit_test";
    protected String name = "test";

    @Autowired
    private RedisClient redisClient;
    private final static Log log = LogFactory.getLog(RedisCache4MysqlTest.class);
}
