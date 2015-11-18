package databus.listener.mysql;

import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;

import databus.event.mysql.AbstractMysqlWriteRow;
import databus.event.mysql.MysqlUpdateRow;
import databus.event.mysql.Column;

public class MysqlUpdateEventFactory extends MysqlWriteEventFactory {
    
    public MysqlUpdateEventFactory(List<String> columns, List<Integer> types,
                            Set<String> primaryKeysSet, List<Pair<Row>> rows) {
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
        Pair<Row> pair= iterator.next();
        ListIterator<com.google.code.or.common.glossary.Column> 
                          befIt = pair.getBefore().getColumns().listIterator();
        ListIterator<com.google.code.or.common.glossary.Column> 
                           aftIt = pair.getAfter().getColumns().listIterator();
        ListIterator<String> columnsIt = columns.listIterator();
        ListIterator<Integer> typesIt = types.listIterator();
        MysqlUpdateRow event = new MysqlUpdateRow();
        while(aftIt.hasNext()) {
            String before = toString(befIt.next());
            String after = toString(aftIt.next());
            int type = typesIt.next();           
            String name = columnsIt.next();
            if (!equals(before, after)) {
                Column column = new Column(name, after, type);
                event.addColumn(column);
            }
            if (primaryKeysSet.contains(name)) {
                Column column = new Column(name, after, type);
                event.addPrimaryKey(column);
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
