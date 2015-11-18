package databus.listener.mysql;

import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import com.google.code.or.common.glossary.Row;

import databus.event.mysql.AbstractMysqlWriteRow;
import databus.event.mysql.Column;
import databus.event.mysql.MysqlInsertRow;


public class MysqlInsertEventFactory extends MysqlWriteEventFactory {
    
    public MysqlInsertEventFactory(List<String> columns, List<Integer> types,
                                  Set<String> primaryKeysSet, List<Row> rows) {
        iterator = rows.listIterator();
        this.columns = columns;
        this.types = types;
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
        ListIterator<String> columnsIt = columns.listIterator();
        ListIterator<Integer> typesIt = types.listIterator();
        AbstractMysqlWriteRow event = newInstance();
        while(rowIt.hasNext()) {
            String value = toString(rowIt.next());
            int type = typesIt.next();
            String name = columnsIt.next();
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
    private List<String> columns;
    private List<Integer> types;
    private Set<String> primaryKeysSet;
}
