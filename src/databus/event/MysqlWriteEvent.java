package databus.event;

import java.util.List;

public interface MysqlWriteEvent<T> extends MysqlEvent{

    public List<T> rows();
    
    public List<String> column() ;
    
}