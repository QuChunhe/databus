package databus.event.mysql;

import databus.event.MysqlEvent;

public class MysqlInsertRow extends AbstractMysqlWriteRow{

    public MysqlInsertRow() {
        super();
    }

    @Override
    public String type() {
        return MysqlEvent.Type.INSERT.toString();
    }
}
