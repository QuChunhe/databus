package databus.event;

import databus.core.Event;

public interface MysqlEvent extends Event{   
    
    enum Type {
        INSERT, UPDATE, DELETE        
    }
    
    long serverId();
    
    String database();
    
    String table();
}
