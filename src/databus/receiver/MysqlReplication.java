package databus.receiver;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import databus.core.Event;
import databus.event.mysql.MysqlDeleteRow;
import databus.event.mysql.MysqlInsertRow;
import databus.event.mysql.MysqlUpdateRow;
import databus.event.mysql.Value;

public class MysqlReplication extends MysqlReceiver{    

    public MysqlReplication() {
        super();
    }

    @Override
    public void receive(Event event) {
        if (event instanceof MysqlInsertRow) {
            insert((MysqlInsertRow) event);
        } else if (event instanceof MysqlUpdateRow) {
            update((MysqlUpdateRow)event);
        } else if (event instanceof MysqlDeleteRow) {
            delete((MysqlDeleteRow)event);
        } else {
            log.error("Can't process "+event.toString());
        }
    }

    private void insert(MysqlInsertRow event) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        sqlBuilder.append(event.table());
        sqlBuilder.append(" (");
        StringBuilder valuesBuilder = new StringBuilder();
        valuesBuilder.append('(');
        Map<String, Value> row = event.row();
        for(String column : row.keySet()) {          
            sqlBuilder.append(column);
            sqlBuilder.append(',');
            Value value = row.get(column);
            append(valuesBuilder, value);
            valuesBuilder.append(',');
        }
        sqlBuilder.deleteCharAt(sqlBuilder.length()-1);
        sqlBuilder.append(')');
        valuesBuilder.deleteCharAt(valuesBuilder.length()-1);
        sqlBuilder.append(')');
        
        sqlBuilder.append(" VALUES ");
        sqlBuilder.append(valuesBuilder);
        
        String sql = sqlBuilder.toString();
        int count = executeWrite(sql);
        
        if (count < 1) {
            log.error(event.toString()+" has't been inserted: "+sql);
        }
    }
    
    private void update(MysqlUpdateRow event) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE ");
        sqlBuilder.append(event.table());
        sqlBuilder.append(" Set ");
        appendEqualFormat(sqlBuilder, event.row());
        sqlBuilder.append(" WHERE ");
        appendEqualFormat(sqlBuilder, event.primaryKeysValue());
        String sql = sqlBuilder.toString();
        int count = executeWrite(sql);
        if (count < 1) {
            log.error(event.toString()+" has't been updated: "+sql);
        }
    }
    
    private void delete(MysqlDeleteRow event) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM ");
        sqlBuilder.append(event.table());
        sqlBuilder.append(" WHERE ");
        appendEqualFormat(sqlBuilder, event.row());
  
        String sql = sqlBuilder.toString();
        int count = executeWrite(sql);
        if (count < 1) {
            log.error(event.toString()+" has't been removed: "+sql);
        }
    }
    
    private StringBuilder appendEqualFormat(StringBuilder builder, 
                                                   Map<String, Value> values) {
        for(String column : values.keySet()) {
            builder.append(column);
            builder.append('=');
            append(builder,values.get(column));
            builder.append(',');
        }
        builder.deleteCharAt(builder.length()-1);
        return builder;
    }

    private void append(StringBuilder builder, Value value) {
        if (value.isString()) {
            if (null == value.value()) {
                builder.append("NULL");
            } else {
                builder.append("'");
                builder.append(value.value().replace("'", "\\'"));
                builder.append("'");
            }
        } else {
            builder.append(value);
        }
    }

    private static Log log = LogFactory.getLog(MysqlReplication.class);
}
