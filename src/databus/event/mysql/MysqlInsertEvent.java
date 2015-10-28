package databus.event.mysql;

import java.util.List;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Row;

import databus.event.MysqlWriteEvent;

public class MysqlInsertEvent extends MysqlAbstractWriteEvent<List<String>> {    
    
    public MysqlInsertEvent(long serverId,String databaseName, 
                            String tableName) {
        super(serverId,databaseName, tableName);
    }
    
    public MysqlInsertEvent() {
        super();
    }   
    
    @Override
    public MysqlWriteEvent<List<String>> setRow(BinlogEventV4 binlogEvent) {
        if (binlogEvent instanceof WriteRowsEventV2) {
            setRows(((WriteRowsEventV2) binlogEvent).getRows());
        }
        return this;
    }

    @Override
    public String type() {
        return Type.INSERT.toString();
    }

    private void setRows(List<Row> binlogRows) {
        for (Row row : binlogRows) {
            rows().add(transform(row));
        }
    }
}
