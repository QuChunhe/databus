package databus.event;

import java.util.List;

import com.google.code.or.binlog.BinlogEventV4;

public interface MysqlWriteEvent<T> extends MysqlEvent{

    public List<T> rows();
    
    public List<String> column() ;
    
    public MysqlWriteEvent<T> setRow(BinlogEventV4 binlogEvent);
    
}