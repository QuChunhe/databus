package databus.receiver;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import databus.core.Event;
import databus.event.mysql.MysqlDeleteRow;
import databus.event.mysql.MysqlInsertRow;
import databus.event.mysql.MysqlUpdateRow;
import databus.event.mysql.Column;

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
        List<Column> row = event.row();
        for(Column column : row) {          
            sqlBuilder.append(column.name());
            sqlBuilder.append(',');
            append(valuesBuilder, column);
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
        appendEqualFormat(sqlBuilder, event.primaryKeys());
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
    
    private void appendEqualFormat(StringBuilder builder, List<Column> row) {
        for(Column column : row) {
            builder.append(column.name());
            builder.append('=');
            append(builder, column);
            builder.append(',');
        }
        builder.deleteCharAt(builder.length()-1);
      }

    private void append(StringBuilder builder, Column column) {
        if (column.isString()) {
            if (null == column.value()) {
                builder.append("NULL");
            } else {
                builder.append("'");
                builder.append(column.value().replace("'", "\\'"));
                builder.append("'");
            }
        } else {
            builder.append(column);
        }
    }

    private static Log log = LogFactory.getLog(MysqlReplication.class);
}
