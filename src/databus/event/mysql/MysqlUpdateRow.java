package databus.event.mysql;

import databus.event.MysqlEvent;

public class MysqlUpdateRow extends AbstractMysqlWriteRow{

    public MysqlUpdateRow() {
        super();
    }

    @Override
    public String type() {
         return MysqlEvent.Type.UPDATE.toString();
    }

}
