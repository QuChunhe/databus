package databus.event.mysql;

import java.util.List;
import java.util.Map;

import databus.event.MysqlEvent;
import databus.event.WriteEvent;

public interface MysqlWriteRow extends WriteEvent, MysqlEvent{

    public Map<String, Value> row();
    
    public List<String> primaryKeys();
    
    public Map<String, Value> primaryKeyValues();
    
}
