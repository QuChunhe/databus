package databus.listener.mysql;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.BlobColumn;

import databus.event.mysql.AbstractMysqlWriteRow;

public abstract class MysqlWriteEventFactory {
    
    abstract public  boolean hasMore();
    
    abstract public AbstractMysqlWriteRow next();
    
    public String toString(Column column) {      
        String value = null;
        if (null != column) {
            if (column instanceof BlobColumn) {
                value = new String(((BlobColumn)column).getValue());
            } else if (column.getValue() !=  null) {
                value = column.toString();
            }
        }
        return value;
    }
}
