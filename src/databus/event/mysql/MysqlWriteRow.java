package databus.event.mysql;

import java.util.List;

import databus.event.MysqlEvent;

public interface MysqlWriteRow extends MysqlEvent{

    public List<Column> row();
    
    public List<Column> primaryKeys();
 
}
