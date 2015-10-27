package databus.event.mysql;

import java.util.LinkedList;
import java.util.List;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;

import databus.event.MysqlWriteEvent;
import databus.util.TableManager;

public abstract class MysqlWriteEventWrapper<T> extends MysqlAbstractEvent 
                                                implements MysqlWriteEvent<T>{

    public MysqlWriteEventWrapper(long serverId,
                                  String databaseName, 
                                  String tableName) {
        this.serverId = serverId;
        this.databaseName = databaseName;
        this.tableName = tableName;
        column = TableManager.instance()
                             .getColumn(databaseName, tableName);
        rows = new LinkedList<T>();
    }    
    
    @Override
    public List<T> rows() {
        return rows;
    }
    @Override
    public List<String> column() {
        return column;
    }

    @Override
    public String toString() {
        String name = this.getClass().getSimpleName()+"="+"["+
                      "time="+time+","+
                      "address="+address()+","+
                      "topic="+topic()+","+
                      "rows="+rows.toString()+
                      "]";
        return name;
    } 
    
    public void setTime(long time) {
        this.time = time;
    }

    protected LinkedList<String> transform(Row row) {
        LinkedList<String> newRow = new LinkedList<String>();
        for(Column c : row.getColumns()) {
            newRow.addLast(c.toString());
        }
        return newRow;
    }

    protected List<T> rows = null;
    protected List<String> column = null; 
}
