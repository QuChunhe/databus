package databus.event.mysql;

import java.util.LinkedList;
import java.util.List;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;

import databus.event.MysqlWriteRows;

public abstract class MysqlAbstractWriteRows<T> extends MysqlAbstractEvent 
                                                implements MysqlWriteRows<T>{

    public MysqlAbstractWriteRows() {
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
        return columnTypes;
    }

    @Override
    public MysqlWriteRows<T> columnNames(List<String> columnNames) {
        this.columnNames = columnNames;
        return this;
    }

    @Override
    public MysqlWriteRows<T> columnTypes(List<Integer> columnTypes) {
        this.columnTypes = columnTypes;
        return this;
    }

    @Override
    public String toString() {
        String name = this.getClass().getSimpleName()+"="+"{"+
                      "time="+time()+";"+
                      "address="+address()+";"+
                      "topic="+topic()+";"+
                      "primaryKeys"+primaryKeys.toString()+";"+
                      "rows="+rows.toString()+";"+
                      "columnNames="+columnNames.toString()+";"+
                      "columntypes="+columnTypes.toString()+";"+
                      "}";
        return name;
    }

    
    @Override
    public List<String> primaryKeys() {
        return primaryKeys;
    }

    @Override
    public MysqlWriteRows<T> primaryKeys(List<String> primaryKey) {
        this.primaryKeys = primaryKey;
        return this;
    }

    protected LinkedList<String> transform(Row row) {
        LinkedList<String> newRow = new LinkedList<String>();
        for(Column c : row.getColumns()) {
            if (c.getValue()==null) {
                newRow.addLast(null);
            } else {
                newRow.addLast(c.toString());
            }
            
        }
        return newRow;
    }

    private List<T> rows = null;
    private List<String> columnNames = null;
    private List<Integer> columnTypes = null;
    private List<String> primaryKeys;
}
