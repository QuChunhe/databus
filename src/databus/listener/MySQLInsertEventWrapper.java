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
        super(serverId,databaseName, tableName);
    }
    
    public void setRows(List<Row> binLogRows) {
        for(Row row : binLogRows) {
            LinkedList<String> newRow = new LinkedList<String>();
            List<Column> columns = row.getColumns();
            for(Column c : columns) {
                newRow.addLast(c.toString());
            }
            rows.add(newRow);
        }        
    }
}
