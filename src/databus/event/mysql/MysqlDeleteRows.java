package databus.event.mysql;

import java.util.List;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.common.glossary.Row;

import databus.event.MysqlWriteRows;

public class MysqlDeleteRows extends MysqlAbstractWriteRows<List<String>> {

    public MysqlDeleteRows() {
        super();
    }

    @Override
    public String type() {
        return Type.DELETE.toString();
    }

    @Override
    public MysqlWriteRows<List<String>> setRows(BinlogEventV4 binlogEvent) {
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
