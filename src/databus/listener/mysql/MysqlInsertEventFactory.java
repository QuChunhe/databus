package databus.listener.mysql;

import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import com.google.code.or.common.glossary.Row;

import databus.event.mysql.AbstractMysqlWriteRow;
import databus.event.mysql.Column;
import databus.event.mysql.MysqlInsertRow;


public class MysqlInsertEventFactory extends MysqlWriteEventFactory {
    
    public MysqlInsertEventFactory(String[] columns, ColumnAttribute[] attributes,
                                  Set<String> primaryKeysSet, List<Row> rows) {
        iterator = rows.listIterator();
        this.columns = columns;
        this.attributes = attributes;
        this.primaryKeysSet = primaryKeysSet;
    }
    
    @Override
    public boolean hasMore() {
        return iterator.hasNext();
    }

    @Override
    public AbstractMysqlWriteRow next() {
        Row row = iterator.next();
        ListIterator<com.google.code.or.common.glossary.Column> 
                                       rowIt = row.getColumns().listIterator();
        AbstractMysqlWriteRow event = newInstance();
        while(rowIt.hasNext()) {
            int index = rowIt.nextIndex();
            ColumnAttribute attribute = attributes[index];
            String value = toString(rowIt.next(), attribute);
            int type = attribute.type();
            String name = columns[index];
            Column column = new Column(name, value, type);  
            event.addColumn(column);
            if (primaryKeysSet.contains(name)) {
                event.addPrimaryKey(column);
            }
        }
        
        return event;
    }
    
    protected AbstractMysqlWriteRow newInstance() {
        return new MysqlInsertRow();
    }

    private ListIterator<Row> iterator;
    private String[] columns;
    private ColumnAttribute[] attributes;
    private Set<String> primaryKeysSet;
}
