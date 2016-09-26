package databus.listener.mysql;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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

public class DuplicateMysqlRowFilter implements EventFilter {    
    
    public DuplicateMysqlRowFilter() {
        super();
    }

    @Override
    public void initialize(Properties properties) {
        String accessExpireTimeValue = properties.getProperty("expireAfterAccess");
        long accessExpireTime = 1;
        if (null != accessExpireTimeValue) {
            accessExpireTime = Long.parseUnsignedLong(accessExpireTimeValue);
        }
        
        String writeExpireTimeValue = properties.getProperty("expireAfterWrite");
        long writeExpireTime = 600;
        if (null != writeExpireTimeValue) {
            writeExpireTime = Long.parseUnsignedLong(writeExpireTimeValue);
        }
        Cache<String, Long> 
                 factory = CacheBuilder.newBuilder()
                                       .softValues()       
                                       .expireAfterWrite(writeExpireTime, TimeUnit.SECONDS)
                                       .expireAfterAccess(accessExpireTime, TimeUnit.SECONDS)
                                       .build();
        cache = factory.asMap();
    }
    
    @Override
    public boolean reject(Event event) {
        log.info(cache.toString());
        if (event instanceof MysqlWriteRow) {
            return exist((MysqlWriteRow) event);
        }
        return false;
    } 
    
    @Override
    public String toString() {
        return getClass().getName();
    }

    public static void put(Event event) {        
        if ((null!=cache) && (event instanceof MysqlWriteRow)) {
            cache.put(toKey((MysqlWriteRow)event), System.currentTimeMillis());
        }
        log.info(cache.toString());
    }
    
    public static boolean exist(MysqlWriteRow event) {
        Long time = cache.get(toKey(event));
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
    
    private static Log log = LogFactory.getLog(DuplicateMysqlRowFilter.class);
    
    private static ConcurrentMap<String, Long> cache = null;
    private static final ColumnComparator COLUMN_COMPARATOR = new ColumnComparator();

}
