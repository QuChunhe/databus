package databus.listener.mysql;

import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;

import databus.event.mysql.AbstractMysqlWriteRow;
import databus.event.mysql.MysqlUpdateRow;
import databus.event.mysql.Value;

public class MysqlUpdateEventFactory extends MysqlWriteEventFactory {
    
    public MysqlUpdateEventFactory(List<String> columns, List<Integer> types,
                                  List<String> primaryKeys, List<Pair<Row>> rows) {
        iterator = rows.listIterator();
        this.columns = columns;
        this.types = types;
        primaryKeysSet = new HashSet<String>(primaryKeys);
    }

    @Override
    public boolean hasMore() {
        return iterator.hasNext();
    }

    @Override
    public AbstractMysqlWriteRow next() {
        Pair<Row> pair= iterator.next();
        ListIterator<Column> befIt = pair.getBefore().getColumns().listIterator();
        ListIterator<Column> aftIt = pair.getAfter().getColumns().listIterator();
        ListIterator<String> columnsIt = columns.listIterator();
        ListIterator<Integer> typesIt = types.listIterator();
        MysqlUpdateRow event = new MysqlUpdateRow();
        while(aftIt.hasNext()) {
            String before = toString(befIt.next());
            String after = toString(aftIt.next());
            int type = typesIt.next();           
            String column = columnsIt.next();
            if (!equals(before, after)) {
                Value value = new Value(after, type);
                event.addValue(column, value);
            }
            if (primaryKeysSet.contains(column)) {
                Value value = new Value(after, type);
                event.addPrimaryKeyValue(column, value);
            }       
        }
        
        return event;
    }
    
    private boolean equals(String one, String other) {
        if (null == one) {
            if (null == other) {
                return true;
            } else {
                return false;
            }
        } else {
            return one.equals(other);
        }
    }
    
    private ListIterator<Pair<Row>> iterator;
    private List<String> columns;
    private List<Integer> types;
    private Set<String> primaryKeysSet;
}
