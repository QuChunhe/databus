package databus.event.mysql;

import java.util.HashMap;
import java.util.Map;

import databus.event.MysqlEvent;

public class MysqlUpdateRow extends AbstractMysqlWriteRow{

    public MysqlUpdateRow() {
        super();
        primaryKeysValue = new HashMap<String, Value>();
    }

    @Override
    public String type() {
         return MysqlEvent.Type.UPDATE.toString();
    }
    
    @Override
    public Map<String, Value> primaryKeysValue() {
        return primaryKeysValue;
    }
    
    public void addPrimaryKeyValue(String primaryKey, Value value) {
        primaryKeysValue.put(primaryKey, value);
    }
    
    private Map<String, Value> primaryKeysValue;
}
