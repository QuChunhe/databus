package databus.receiver;

import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.event.mysql.MysqlDeleteEvent;
import databus.event.mysql.MysqlInsertEvent;
import databus.event.mysql.MysqlUpdateEvent;

public class MysqlReplication extends MysqlReceiver{    

    public MysqlReplication() {
        super();
    }

    @Override
    public void receive(Event event) {
        if (event instanceof MysqlInsertEvent) {
            insert((MysqlInsertEvent) event);
        } else if (event instanceof MysqlUpdateEvent) {
            update((MysqlUpdateEvent)event);
        } else if (event instanceof MysqlDeleteEvent) {
            delete((MysqlDeleteEvent)event);
        } else {
            log.error("Can't process "+event.toString());
        }
    }

    private void insert(MysqlInsertEvent event) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        sqlBuilder.append(event.tableName().toLowerCase());
        sqlBuilder.append(" ");
        sqlBuilder.append(toInsertPhrase(event.columnNames(),EMPTY_INDEX_SET));        
        sqlBuilder.append(" VALUES ");
        Set<Integer> indexSet = stringIndexSet(event.columnTypes());
        for(List<String> row : event.rows()) {
            sqlBuilder.append(toInsertPhrase(row,indexSet));
            sqlBuilder.append(",");
        }
        sqlBuilder.deleteCharAt(sqlBuilder.length()-1);
        String sql = sqlBuilder.toString();
        int count = executeWrite(sql);
        if (event.rows().size() != count) {
            log.error(count+" rows has been inserted: "+sql);
        }
        log.info(sql);
    }
    
    private void update(MysqlUpdateEvent event) {
        Set<Integer> indexSet = stringIndexSet(event.columnTypes());
        List<String> columnNames = event.columnNames();
        for(MysqlUpdateEvent.Entity entity : event.rows()) {
            String phase = toUpdatePhrase(entity, columnNames, indexSet);
            if (null != phase) {
                StringBuilder sqlBuilder = new StringBuilder();
                sqlBuilder.append("UPDATE ");
                sqlBuilder.append("event.tableName().toLowerCase()"); 
                sqlBuilder.append(" ");
                sqlBuilder.append(phase);
            }
            
            
        }       
        
    }
    
    private void delete(MysqlDeleteEvent event) {
        
    }
    
    private Set<Integer> stringIndexSet(List<Integer> types) {
        Set<Integer> indexSet = new HashSet<Integer>();
        ListIterator<Integer> it = types.listIterator();
        while(it.hasNext()) {
            Integer index = it.nextIndex();
            int t = it.next();
            if((12==t) ||(1==t)) {
                indexSet.add(index);
            }
        }
        return indexSet;
    }
    
    private String toInsertPhrase(List<String> row, Set<Integer> indexSet) {
        log.info(row.getClass().getName());
        StringBuilder builder = new StringBuilder();
        builder.append('(');
        ListIterator<String> it = row.listIterator();
        while(it.hasNext()) {
            int index = it.nextIndex();
            String value = it.next();
            append(builder, value, indexSet.contains(index));            
            builder.append(',');
        }
        builder.deleteCharAt(builder.length()-1);
        builder.append(')');
        
        return builder.toString();
    }
    
    private String toUpdatePhrase(MysqlUpdateEvent.Entity entity, 
                             List<String> columnNames, Set<Integer> indexSet) {
        StringBuilder builder = new StringBuilder();
        ListIterator<String> nameIt = columnNames.listIterator();
        ListIterator<String> beforeIt = entity.before().listIterator();
        ListIterator<String> afterIt = entity.after().listIterator();
        while(nameIt.hasNext()) {
            String before = beforeIt.next();
            String after = afterIt.next();
            if (equals(before,after)) {
                continue;
            }
            int index = nameIt.nextIndex();
            builder.append(nameIt.next());
            builder.append('=');
            append(builder, after, indexSet.contains(index));
            builder.append(',');
        }
        builder.deleteCharAt(builder.length()-1);
        
        return builder.toString();
    }
    
    private void append(StringBuilder builder, String value, boolean isString) {
        if (!isString) {
            builder.append(value);
        } else {
            if (null == value) {
                builder.append("NULL");
            } else {
                builder.append("'");
                builder.append(value);
                builder.append("'");
            }
        }
    }
    
    private boolean equals(String one, String other) {
        if (null == one) {
            if (null == other) {
                return true;
            } else {
                return false;
            }
        } else {
            return one.equals(other);
        }
    }
    
    private static Log log = LogFactory.getLog(MysqlReplication.class);
    private Set<Integer> EMPTY_INDEX_SET = new HashSet<Integer>();

}
