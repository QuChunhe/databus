package databus.listener.mysql;

import java.util.List;
import java.util.ListIterator;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;

import databus.event.mysql.AbstractMysqlWriteRow;
import databus.event.mysql.MysqlInsertRow;
import databus.event.mysql.Value;


public class MysqlInsertEventFactory extends MysqlWriteEventFactory {
    
    public MysqlInsertEventFactory(List<String> columns, List<Integer> types,
                                                              List<Row> rows) {
        iterator = rows.listIterator();
        this.columns = columns;
        this.types = types;
    }
    
    @Override
    public boolean hasMore() {
        return iterator.hasNext();
    }

    @Override
    public AbstractMysqlWriteRow next() {
        Row row = iterator.next();
        ListIterator<Column> rowIt = row.getColumns().listIterator();
        ListIterator<String> columnsIt = columns.listIterator();
        ListIterator<Integer> typesIt = types.listIterator();
        AbstractMysqlWriteRow event = newInstance();
        while(rowIt.hasNext()) {
            Column c = rowIt.next();
            String v = toString(c);
            int t = typesIt.next();
            Value value = new Value(v, t);
            String column = columnsIt.next();
            event.addValue(column, value);            
        }
        
        return event;
    }
    
    protected AbstractMysqlWriteRow newInstance() {
        return new MysqlInsertRow();
    }

    private ListIterator<Row> iterator;
    private List<String> columns;
    private List<Integer> types;
}
