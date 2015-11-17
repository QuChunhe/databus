package databus.event.mysql;

import databus.event.MysqlEvent;

public class MysqlDeleteRow extends AbstractMysqlWriteRow{
    
    public MysqlDeleteRow() {
        super();
    }

    @Override
    public String type() {
        return MysqlEvent.Type.DELETE.toString();
    }
}
