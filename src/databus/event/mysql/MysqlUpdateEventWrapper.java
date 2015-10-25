package databus.event.mysql;

import java.util.LinkedList;
import java.util.List;

import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;

import databus.event.MysqlUpdateEvent;


public class MysqlUpdateEventWrapper 
                       extends MysqlWriteEventWrapper<MysqlUpdateEvent.Entity>
                       implements MysqlUpdateEvent{

    public MysqlUpdateEventWrapper(long serverId, String databaseName,
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
    
    @Override
    public String type() {
        return Type.UPDATE.toString();
    }

}
