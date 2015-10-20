package databus.event;

import java.util.LinkedList;
import java.util.List;

import databus.util.TableManager;

public abstract class MySQLWriteEvent<T> extends MySQLAbstractEvent{    

    public MySQLWriteEvent(long serverId,
                           String databaseName, 
                           String tableName) {
        super();
        this.serverId = serverId;
        this.databaseName = databaseName;
        this.tableName = tableName;
        column = TableManager.instance()
                             .getColumn(databaseName, tableName);
        rows = new LinkedList<T>();
    }

    public List<T> rows() {
        return rows;
    }
    
    public List<String> column() {
        return column;
    }
    
    
    public void setTime(long time) {
        this.time = time;
    }
    
    protected List<T> rows = null;
    protected List<String> column = null; 
}
