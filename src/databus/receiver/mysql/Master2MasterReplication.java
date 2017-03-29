package databus.receiver.mysql;

import java.io.IOException;
import java.sql.Connection;

import databus.core.Event;
import databus.event.mysql.MysqlWriteRow;
import databus.listener.mysql.DuplicateRowFilter;

public class Master2MasterReplication extends MysqlReplication {

    public Master2MasterReplication() {
        super();
    }

    @Override
    public void close() throws IOException {
        duplicateRowFilter.store();
        super.close();
    }

    @Override
    protected String execute(Connection conn, Event event) {
        if (event instanceof MysqlWriteRow) {
            MysqlWriteRow mysqlWriteRow = (MysqlWriteRow)event;
            if (duplicateRowFilter.isFilteredTable(mysqlWriteRow.table().toLowerCase())) {
                duplicateRowFilter.putIfAbsentOrIncrementIfPresent(mysqlWriteRow);
            }
        }
        return super.execute(conn, event);
    }

    public void setDuplicateRowFilter(DuplicateRowFilter duplicateRowFilter) {
        this.duplicateRowFilter = duplicateRowFilter;
    }

    private DuplicateRowFilter duplicateRowFilter;
}
