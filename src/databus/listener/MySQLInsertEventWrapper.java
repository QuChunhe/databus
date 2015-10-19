package databus.listener;

import java.util.LinkedList;
import java.util.List;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;

import databus.event.MySQLInsertEvent;

public class MySQLInsertEventWrapper extends MySQLInsertEvent{    
    
    public MySQLInsertEventWrapper(long serverId,
                                  String databaseName, 
                                  String tableName) {
        this.serverId = serverId;
        this.databaseName = databaseName;
        this.tableName = tableName;
        rows = new LinkedList<List<String>>();
    }
    
    public void setRows(List<Row> rows) {
        for(Row row : rows) {
            LinkedList<String> newRow = new LinkedList<String>();
            List<Column> columns = row.getColumns();
            for(Column column : columns) {
                newRow.addLast(column.toString());
            }
            this.rows.add(newRow);
        }
        
    }
}
