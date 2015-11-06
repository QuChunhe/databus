package databus.event.mysql;

import java.util.LinkedList;
import java.util.List;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;

import databus.event.MysqlWriteRows;

public class MysqlUpdateRows 
                     extends MysqlAbstractWriteRows<MysqlUpdateRows.Entity> {

    public static class Entity {        
        
        public Entity(List<String> before, List<String> after) {
            this.before = before;
            this.after = after;
        }
        
        public List<String> before() {
            return before;
        }
        
        public List<String> after() {
            return after;
        }
        
        @Override
        public String toString() {
            return "[before="+before.toString()+";"+
                   "after="+after.toString()+"]";
        }

        private List<String> before;
        private List<String> after;
    }
    
    public MysqlUpdateRows() {
        super();
    }
    
    @Override
    public MysqlWriteRows<Entity> setRows(BinlogEventV4 binlogEvent) {
        if (binlogEvent instanceof UpdateRowsEventV2) {
            setRows(((UpdateRowsEventV2) binlogEvent).getRows());
        }
        return this;
    }
    
    @Override
    public String type() {
        return Type.UPDATE.toString();
    }

    private void setRows(List<Pair<Row>> binLogRows) {
        for(Pair<Row> pair : binLogRows) {
            LinkedList<String> before = transform(pair.getBefore());
            LinkedList<String> after = transform(pair.getAfter());
            rows().add(new Entity(before,after));
        }        
    }
}
