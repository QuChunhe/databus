package databus.event.mysql;

import java.util.LinkedList;
import java.util.List;

public abstract class AbstractMysqlWriteRow  extends AbstractMysqlEvent 
                                               implements MysqlWriteRow{
    
    public AbstractMysqlWriteRow() {
        super();
        row = new LinkedList<Column>();
        primaryKeys = new LinkedList<Column>();
    }

    @Override
    public List<Column> row() {
        return row;
    }

    @Override
    public List<Column> primaryKeys() {
        return primaryKeys;
    }
    
    public AbstractMysqlWriteRow addPrimaryKey(Column primaryKey) {
        primaryKeys.add(primaryKey);
        return this;
    }
    
    public AbstractMysqlWriteRow addColumn(Column column) {
        row.add(column);
        return this;
    }

    @Override
    public String toString() {
        return type()+" from "+ database()+"."+table()+" : "+row.toString();
    } 

    private List<Column> row;
    private List<Column> primaryKeys;
}
