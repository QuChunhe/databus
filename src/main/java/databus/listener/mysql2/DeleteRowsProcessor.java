package databus.listener.mysql2;

import databus.event.mysql.AbstractMysqlWriteRow;
import databus.event.mysql.MysqlDeleteRow;

/**
 * Created by Qu Chunhe on 2018-04-16.
 */
public class DeleteRowsProcessor extends InsertRowsProcessor {
    public DeleteRowsProcessor() {
    }

    @Override
    protected AbstractMysqlWriteRow newInstance() {
        return new MysqlDeleteRow();
    }
}
