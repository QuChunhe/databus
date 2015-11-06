package databus.event.mysql;

import java.util.List;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Row;

import databus.event.MysqlWriteRows;

public class MysqlInsertRows extends MysqlAbstractWriteRows<List<String>> {    

    public MysqlInsertRows() {
        super();
    }   
    
    @Override
    public MysqlWriteRows<List<String>> setRows(BinlogEventV4 binlogEvent) {
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
