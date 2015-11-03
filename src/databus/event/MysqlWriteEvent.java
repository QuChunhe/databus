package databus.event;

import java.util.List;

import com.google.code.or.binlog.BinlogEventV4;

public interface MysqlWriteEvent<T> extends MysqlEvent{

    public List<T> rows();
    
    public List<String> columnNames();
    
    public MysqlWriteEvent<T> columnNames(List<String> columnNames);
    
    public List<Integer> columnTypes();
    
    public MysqlWriteEvent<T> columnTypes(List<Integer> columnTypes);
    
    public List<String> primaryKeys();
    
    public MysqlWriteEvent<T> primaryKeys(List<String> primaryKeys);
    
    public MysqlWriteEvent<T> setRows(BinlogEventV4 binlogEvent);
    
}