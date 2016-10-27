package databus.listener.mysql;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.Map;

import databus.core.Listener;
import databus.core.Stoppable;
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

public class DuplicateMysqlRowFilter implements EventFilter, Stoppable {
    
    public DuplicateMysqlRowFilter() {
        super();
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
                                       .expireAfterAccess(1, TimeUnit.SECONDS)
                                       .build();
        cache = factory.asMap();
        if (new File(BACKUP_FILE_NAME).exists()) {
            loadCache();
        }
    }
    
    @Override
    public boolean doesReject(Event event) {
        log.info(cache.toString());
        if (event instanceof MysqlWriteRow) {
            return exist((MysqlWriteRow) event);
        }
        return false;
    }

    @Override
    public void setListener(Listener listener) {
        if (listener instanceof MysqlListener) {
            mysqlListener = (MysqlListener) listener;
        } else {
            log.error(listener.getClass().getName()+" isn't instance of MysqlListener!");
            System.exit(1);
        }
    }

    @Override
    public void stop() {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(BACKUP_FILE_NAME),
                                                             StandardCharsets.UTF_8,
                                                             StandardOpenOption.CREATE,
                                                             StandardOpenOption.TRUNCATE_EXISTING,
                                                             StandardOpenOption.WRITE);) {
            Properties p = new Properties();
            p.putAll(cache);
            p.store(writer, new Date().toString() );
            writer.flush();
        } catch (IOException e) {
            log.error("Can't write "+ BACKUP_FILE_NAME, e);
        }
    }

    @Override
    public String toString() {
        return getClass().getName();
    }

    public static void put(Event event) {
        if (event instanceof MysqlWriteRow) {
        } else {
            log.error(event.getClass().getName()+" is not MysqlWriteRow ");
            return;
        }
        MysqlWriteRow e = (MysqlWriteRow)event;
        if (!mysqlListener.doesPermit(e.database().toLowerCase()+"."+e.table().toLowerCase())) {
            log.info("not put cache");
            return;
        }
        if (null != cache) {
            cache.put(toKey(e), System.currentTimeMillis());
        }
        log.info(cache.toString());
    }
    
    public static boolean exist(MysqlWriteRow event) {
        String key = toKey(event);
        Long time = cache.get(key);
        cache.remove(key, time);
        return time != null;
    }
    
    private static String toKey(MysqlWriteRow event) {
        List<Column> primaryKeys = event.primaryKeys();
        Column[] orderedPrimaryKeys = primaryKeys.toArray(new Column[primaryKeys.size()]);
        Arrays.sort(orderedPrimaryKeys, COLUMN_COMPARATOR);
        StringBuilder builder = new StringBuilder(128);
        builder.append(event.table())
               .append(":")
               .append(event.type())
               .append(":");
        for(Column c : orderedPrimaryKeys) {
            builder.append(c.name())
                   .append("=")
                   .append(c.value())
                   .append("&");
        }
        if (MysqlEvent.Type.UPDATE.toString().equals(event.type())) {
            List<Column> row = event.row();
            Column[] orderedColumns = row.toArray(new Column[row.size()]);
            Arrays.sort(orderedColumns, COLUMN_COMPARATOR);
            for(Column c : orderedColumns) {
                builder.append(c.name())
                       .append("=")
                       .append(c.value())
                       .append("&");
            }
        }
        return builder.substring(0, builder.length()-1);
    }

    private void loadCache() {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(BACKUP_FILE_NAME),
                                                             StandardCharsets.UTF_8);) {
            Properties p = new Properties();
            p.load(reader);
            for(Map.Entry<Object, Object> entry : p.entrySet()) {
                cache.put(entry.getKey().toString(),
                          Long.parseUnsignedLong(entry.getValue().toString()));
            }
        } catch (IOException e) {
            log.error("Can't load cache data from "+BACKUP_FILE_NAME, e);
        }
    }
    
    private static Log log = LogFactory.getLog(DuplicateMysqlRowFilter.class);
    
    private static ConcurrentMap<String, Long> cache = null;
    private static final ColumnComparator COLUMN_COMPARATOR = new ColumnComparator();
    private static MysqlListener mysqlListener = null;

    private final String BACKUP_FILE_NAME = "data/master2master_replication_cache_backup.data";
}
