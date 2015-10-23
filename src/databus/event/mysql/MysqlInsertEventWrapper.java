package databus.event.mysql;

import java.util.List;

import com.google.code.or.common.glossary.Row;

import databus.event.MysqlEvent;
import databus.event.MysqlInsertEvent;


public class MysqlInsertEventWrapper extends MysqlWriteEventWrapper<List<String>>
                                     implements MysqlInsertEvent{    
    
    public MysqlInsertEventWrapper(long serverId,String databaseName, 
                                   String tableName) {
        super(serverId,databaseName, tableName);
    }
    
    public void setRows(List<Row> binLogRows) {
        for(Row row : binLogRows) {            
            rows.add(transform(row));
        }        
    }

    @Override
    public Type type() {
        return MysqlEvent.Type.INSERT;
    }
}
