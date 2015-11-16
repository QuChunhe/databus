package databus.event.mysql;

import java.util.LinkedList;
import java.util.List;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.BlobColumn;

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
    public List<String> columns() {
        return columns;
    }    
    
    @Override
    public List<Integer> columnTypes() {
        return types;
    }

    @Override
    public MysqlWriteRows<T> columns(List<String> columns) {
        this.columns = columns;
        return this;
    }

    @Override
    public MysqlWriteRows<T> columnTypes(List<Integer> columnTypes) {
        this.types = columnTypes;
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
                      "columns="+columns.toString()+";"+
                      "types="+types.toString()+";"+
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
                String value;
                if (c instanceof BlobColumn) {
                    value = new String(((BlobColumn)c).getValue());
                } else {
                    value = c.toString();
                }
                newRow.addLast(value);
            }
            
        }
        return newRow;
    }

    private List<T> rows = null;
    private List<String> columns = null;
    private List<Integer> types = null;
    private List<String> primaryKeys;
}
