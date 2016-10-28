package databus.receiver.mysql;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;

import databus.core.Event;
import databus.event.mysql.MysqlWriteRow;
import databus.listener.mysql.DuplicateRowFilter;

public class Master2MasterReplication extends MysqlReplication implements Closeable{

    public Master2MasterReplication() {
        super();
    }

    @Override
    public void close() throws IOException {
        filter.store();
    }

    @Override
    protected void receive(Connection conn, Event event) {
        if (event instanceof MysqlWriteRow) {
            MysqlWriteRow mysqlWriteRow = (MysqlWriteRow)event;
            if (filter.isFilteredTable(mysqlWriteRow.table().toLowerCase())) {
                filter.put(mysqlWriteRow);
            }
            super.receive(conn, event);
        }
    }

    public void setDuplicateRowFilter(DuplicateRowFilter filter) {
        this.filter = filter;
    }


    private DuplicateRowFilter filter;
}