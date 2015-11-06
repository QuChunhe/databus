package databus.event;

import java.util.List;

import com.google.code.or.binlog.BinlogEventV4;

public interface MysqlWriteRows<T> extends MysqlEvent{

    public List<T> rows();
    
    public List<String> columnNames();
    
    public MysqlWriteRows<T> columnNames(List<String> columnNames);
    
    public List<Integer> columnTypes();
    
    public MysqlWriteRows<T> columnTypes(List<Integer> columnTypes);
    
    public List<String> primaryKeys();
    
    public MysqlWriteRows<T> primaryKeys(List<String> primaryKeys);
    
    public MysqlWriteRows<T> setRows(BinlogEventV4 binlogEvent);
    
}