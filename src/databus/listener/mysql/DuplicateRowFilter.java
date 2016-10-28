package databus.listener.mysql;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import databus.core.Event;
import databus.core.EventFilter;
import databus.event.MysqlEvent;
import databus.event.mysql.Column;
import databus.event.mysql.ColumnComparator;
import databus.event.mysql.MysqlWriteRow;

public class DuplicateRowFilter implements EventFilter {
    
    public DuplicateRowFilter() {
        super();
        filteredTableSet = new HashSet<String>();
    }

    @Override
    public void initialize(Properties properties) {
        String writeExpireTimeValue = properties.getProperty("expireAfterWrite");
        long writeExpireTime = 600;
        if (null != writeExpireTimeValue) {
            writeExpireTime = Long.parseUnsignedLong(writeExpireTimeValue);
        }
        Cache<String, Long>
                 factory = CacheBuilder.newBuilder()
                                       .softValues()       
                                       .expireAfterWrite(writeExpireTime, TimeUnit.SECONDS)
                                       .build();
        cache = factory.asMap();
    }
    
    @Override
    public boolean doesReject(Event event) {
        log.info(cache.toString());
        if (event instanceof MysqlWriteRow) {
            return removeIfExist((MysqlWriteRow) event);
        }
        return false;
    }

    public void put(MysqlWriteRow event) {
        cache.put(toKey(event), System.currentTimeMillis());
        log.info(cache.toString());
    }

    public void store() {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(backupFileName),
                                                             StandardCharsets.UTF_8,
                                                             StandardOpenOption.CREATE,
                                                             StandardOpenOption.TRUNCATE_EXISTING,
                                                             StandardOpenOption.WRITE);) {
            Properties p = new Properties();
            p.putAll(cache);
            p.store(writer, "Filter Duplicate Row");
        } catch (IOException e) {
            log.error("Can't write "+ backupFileName, e);
        }
    }

    public void load() {
        File backupFile = new File(backupFileName);
        if (!backupFile.exists()) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(backupFile.toPath(),
                                                             StandardCharsets.UTF_8);) {
            Properties p = new Properties();
            p.load(reader);
            for(Map.Entry<Object, Object> entry : p.entrySet()) {
                cache.put(entry.getKey().toString(),
                          Long.parseUnsignedLong(entry.getValue().toString()));
            }
        } catch (IOException e) {
            log.error("Can't load cache data from "+ backupFileName, e);
        }
    }

    public void addFilteredTable(String table) {
        filteredTableSet.add(table);
    }

    public boolean isFilteredTable(String table) {
        return filteredTableSet.contains(table);
    }

    @Override
    public String toString() {
        return getClass().getName();
    }
    
    private boolean removeIfExist(MysqlWriteRow event) {
        String key = toKey(event);
        Long time = cache.get(key);
        if (null != time) {
            cache.remove(key, time);
        }
        return time != null;
    }
    
    private String toKey(MysqlWriteRow event) {
        List<Column> primaryKeys = event.primaryKeys();
        Column[] sortedPrimaryKeys = primaryKeys.toArray(new Column[primaryKeys.size()]);
        Arrays.sort(sortedPrimaryKeys, COLUMN_COMPARATOR);
        StringBuilder builder = new StringBuilder(128);
        builder.append(event.table())
               .append(":")
               .append(event.type())
               .append(":");
        for(Column c : sortedPrimaryKeys) {
            builder.append(c.name())
                   .append("=")
                   .append(c.value())
                   .append("&");
        }
        if (MysqlEvent.Type.UPDATE.toString().equals(event.type())) {
            List<Column> row = event.row();
            Column[] sortedColumns = row.toArray(new Column[row.size()]);
            Arrays.sort(sortedColumns, COLUMN_COMPARATOR);
            for(Column c : sortedColumns) {
                builder.append(c.name())
                       .append("=")
                       .append(c.value())
                       .append("&");
            }
        }
        return builder.substring(0, builder.length()-1);
    }
    
    private static Log log = LogFactory.getLog(DuplicateRowFilter.class);

    private final ColumnComparator COLUMN_COMPARATOR = new ColumnComparator();
    private final String backupFileName = "data/master2master_replication_cache_backup.data";

    private ConcurrentMap<String, Long> cache = null;
    private Set<String> filteredTableSet;
}
