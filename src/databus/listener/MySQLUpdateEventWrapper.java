package databus.listener;

import java.util.LinkedList;
import java.util.List;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;

import databus.event.MySQLUpdateEvent;

public class MySQLUpdateEventWrapper extends MySQLUpdateEvent{

    public MySQLUpdateEventWrapper(long serverId, String databaseName,
                                   String tableName) {
        super(serverId, databaseName, tableName);
    }
    
    public void setRows(List<Pair<Row>> binLogRows) {
        for(Pair<Row> pair : binLogRows) {
            LinkedList<String> before = transform(pair.getBefore());
            LinkedList<String> after = transform(pair.getAfter());
            rows.add(new Entity(before,after));
        }        
    }
    
    private LinkedList<String> transform(Row row) {
        LinkedList<String> newRow = new LinkedList<String>();
        for(Column c : row.getColumns()) {
            newRow.addLast(c.toString());
        }
        return newRow;
    }

}
