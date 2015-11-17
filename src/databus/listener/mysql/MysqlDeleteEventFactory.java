package databus.listener.mysql;

import java.util.List;

import com.google.code.or.common.glossary.Row;

import databus.event.mysql.AbstractMysqlWriteRow;
import databus.event.mysql.MysqlDeleteRow;

public class MysqlDeleteEventFactory extends MysqlInsertEventFactory{

    public MysqlDeleteEventFactory(List<String> columns, List<Integer> types,
                                                              List<Row> rows) {
        super(columns, types, rows);
    }
    
    @Override
    protected AbstractMysqlWriteRow newInstance() {
        return new MysqlDeleteRow();
    }
}
