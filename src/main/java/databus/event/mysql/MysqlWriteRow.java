package databus.event.mysql;

import java.util.List;

import databus.event.MysqlEvent;

public interface MysqlWriteRow extends MysqlEvent{

    List<Column> row();
    
    List<Column> primaryKeys();
 
}
