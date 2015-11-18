package databus.event.mysql;

import java.util.List;

import databus.event.MysqlEvent;
import databus.event.WriteEvent;

public interface MysqlWriteRow extends WriteEvent, MysqlEvent{

    public List<Column> row();
    
    public List<Column> primaryKeys();
 
}
