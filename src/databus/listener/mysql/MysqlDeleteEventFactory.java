package databus.listener.mysql;

import java.util.List;
import java.util.Set;

import com.google.code.or.common.glossary.Row;

import databus.event.mysql.AbstractMysqlWriteRow;
import databus.event.mysql.MysqlDeleteRow;

public class MysqlDeleteEventFactory extends MysqlInsertEventFactory{

    public MysqlDeleteEventFactory(String[] columns, ColumnAttribute[] attributes,
                                  Set<String> primaryKeysSet, List<Row> rows) {
        super(columns, attributes, primaryKeysSet, rows);
    }
    
    @Override
    protected AbstractMysqlWriteRow newInstance() {
        return new MysqlDeleteRow();
    }
}
