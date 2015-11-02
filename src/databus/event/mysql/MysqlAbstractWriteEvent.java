package databus.event.mysql;

import java.util.LinkedList;
import java.util.List;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;

import databus.event.MysqlWriteEvent;

public abstract class MysqlAbstractWriteEvent<T> extends MysqlAbstractEvent 
                                                 implements MysqlWriteEvent<T>{

    public MysqlAbstractWriteEvent() {
        rows = new LinkedList<T>();
    } 
        
    @Override
    public List<T> rows() {
        return rows;
    }

    @Override
    public List<String> columnNames() {
        return columnNames;
    }    
    
    @Override
    public List<Integer> columnTypes() {
        return columnTyps;
    }

    @Override
    public MysqlWriteEvent<T> columnNames(List<String> columnNames) {
        this.columnNames = columnNames;
        return this;
    }

    @Override
    public MysqlWriteEvent<T> columnTypes(List<Integer> columnTypes) {
        this.columnTyps = columnTypes;
        return this;
    }

    @Override
    public String toString() {
        String name = this.getClass().getSimpleName()+"="+"["+
                      "time="+time()+","+
                      "address="+address()+","+
                      "topic="+topic()+","+
                      "rows="+rows.toString()+
                      "]";
        return name;
    }

    protected LinkedList<String> transform(Row row) {
        LinkedList<String> newRow = new LinkedList<String>();
        for(Column c : row.getColumns()) {
            newRow.addLast(c.toString());
        }
        return newRow;
    }

    private List<T> rows = null;
    private List<String> columnNames = null;
    private List<Integer> columnTyps = null;
}
