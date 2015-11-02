package databus.event.mysql;

import java.util.List;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.common.glossary.Row;

import databus.event.MysqlWriteEvent;

public class MysqlDeleteEvent extends MysqlAbstractWriteEvent<List<String>> {

    public MysqlDeleteEvent() {
        super();
    }

    @Override
    public String type() {
        return Type.DELETE.toString();
    }

    @Override
    public MysqlWriteEvent<List<String>> setRows(BinlogEventV4 binlogEvent) {
        if (binlogEvent instanceof DeleteRowsEventV2) {
            setRows(((DeleteRowsEventV2) binlogEvent).getRows());
        }
        return this;
    }
    
    private void setRows(List<Row> binlogRows) {
        for (Row row : binlogRows) {
            rows().add(transform(row));
        }
    }
}
