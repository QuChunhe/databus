package databus.event.mysql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public abstract class AbstractMysqlWriteRow  extends AbstractMysqlEvent 
                                               implements MysqlWriteRow{
    
    public AbstractMysqlWriteRow() {
        super();
        row = new HashMap<String, Value>();
    }

    @Override
    public Map<String, Value> row() {
        return row;
    }

    @Override
    public List<String> primaryKeys() {
        return primaryKeys;
    }
    
    public AbstractMysqlWriteRow primaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }
    
    public AbstractMysqlWriteRow addValue(String column, Value value) {
        row.put(column, value);
        return this;
    }

    @Override
    public String toString() {
        return type()+" from "+ database()+"."+table()+" : "+row.toString();
    } 

    @Override
    public Map<String, Value> primaryKeyValues() {
        HashSet<String> primaryKeysSet = new HashSet<String>(primaryKeys);
        Map<String, Value> values = new HashMap<String, Value>();
        for(String column : row.keySet()) {
            if (primaryKeysSet.contains(column)) {
                values.put(column, row.get(column));
            }
        }
        return values;
    }

    private Map<String, Value> row;
    private List<String>  primaryKeys;
}
