package databus.event.mysql;

import java.util.List;

import com.google.code.or.common.glossary.Row;

import databus.event.MysqlDeleteEvent;

public class MysqlDeleteEventWrapper extends MysqlWriteEventWrapper<List<String>>
                                     implements MysqlDeleteEvent {

    public MysqlDeleteEventWrapper(long serverId, String databaseName,
                                   String tableName) {
        super(serverId, databaseName, tableName);
    }

    public void setRows(List<Row> binLogRows) {
        for(Row row : binLogRows) {            
            rows.add(transform(row));
        }        
    }
    
    @Override
    public String type() {
        return Type.DELETE.toString();
    }   

}
