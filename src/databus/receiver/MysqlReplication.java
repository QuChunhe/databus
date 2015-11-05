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
        sqlBuilder.append(convertString(event.columnNames(),EMPTY_INDEX_SET));        
         sqlBuilder.append(" VALUES ");
        Set<Integer> indexSet = stringIndexSet(event.columnTypes());
        for(List<String> row : event.rows()) {
            sqlBuilder.append(convertString(row,indexSet));
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
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE ");
        sqlBuilder.append("event.tableName().toLowerCase()");
        
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
    
    private String convertString(List<String> row, Set<Integer> indexSet) {
        StringBuilder builder = new StringBuilder();
        builder.append('(');
        ListIterator<String> it = row.listIterator();
        while(it.hasNext()) {
            String element=it.next();
            if (null == element) {
                builder.append("NULL");
            } else {
                boolean isString = indexSet.contains(it.previousIndex());
                if (isString) {
                    builder.append("'");
                }
                builder.append(element);
                if (isString) {
                    builder.append("'");
                }
            }            
            builder.append(',');
        }
        builder.deleteCharAt(builder.length()-1);
        builder.append(')');
        
        return builder.toString();
    }
    
    private String convertString(List<String> before, List<String> after, 
                             List<String> columnNames, Set<Integer> indexSet) {
        
        return null;
    }
    
    private static Log log = LogFactory.getLog(MysqlReplication.class);
    private Set<Integer> EMPTY_INDEX_SET = new HashSet<Integer>();

}
