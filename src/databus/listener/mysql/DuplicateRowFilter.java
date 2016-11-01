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
import java.util.concurrent.atomic.AtomicInteger;

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
        Cache<String, AtomicInteger>
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
            return existAndRemoveIfZero((MysqlWriteRow) event);
        }
        return false;
    }

    public void putIfAbsentOrIncrementIfPresent(MysqlWriteRow event) {
        String key = toKey(event);
        final AtomicInteger ONE = new AtomicInteger(1);
        AtomicInteger preCount;
        AtomicInteger count = null;
        do {
            preCount = cache.putIfAbsent(key, ONE);
            if (null != preCount) {
                synchronized (preCount) {
                    count = cache.get(key);
                    if (count == preCount) {
                        count.incrementAndGet();
                    }
                }
            }
        } while (preCount != count);

        log.info(cache.toString());
    }

    public void store() {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(backupFileName),
                                                             StandardCharsets.UTF_8,
                                                             StandardOpenOption.CREATE,
                                                             StandardOpenOption.TRUNCATE_EXISTING,
                                                             StandardOpenOption.WRITE);) {
            Properties properties = new Properties();
            properties.putAll(cache);
            properties.store(writer, "Filter Duplicate Row");
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
            Properties properties = new Properties();
            properties.load(reader);
            for(Map.Entry<Object, Object> entry : properties.entrySet()) {
                cache.put(entry.getKey().toString(),
                          new AtomicInteger(Integer.parseUnsignedInt(entry.getValue()
                                                                          .toString())));
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
    
    private boolean existAndRemoveIfZero(MysqlWriteRow event) {
        String key = toKey(event);
        AtomicInteger count = cache.get(key);
        if (null == count) {
            return false;
        }
        if (count.decrementAndGet() == 0) {
            synchronized (count) {
                if (count.get() == 0) {
                    cache.remove(key, count);
                }
            }
        }
        return true;
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

    private ConcurrentMap<String, AtomicInteger> cache = null;
    private Set<String> filteredTableSet;
}
