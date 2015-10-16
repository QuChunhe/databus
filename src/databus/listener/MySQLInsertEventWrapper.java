package databus.listener;

import java.util.List;

import com.google.code.or.common.glossary.Row;

import databus.event.MySQLInsertEvent;

public class MySQLInsertEventWrapper extends MySQLInsertEvent{    
    
    public MySQLInsertEventWrapper(String data) {
        
    }

    public MySQLInsertEventWrapper setRows(List<Row> rows) {
        return this;
    }
    
    public void clear() {
        
    }
    

}
