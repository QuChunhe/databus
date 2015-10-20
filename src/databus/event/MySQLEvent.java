package databus.event;

import databus.core.Event;

public interface MySQLEvent  extends Event{   
    
    static public enum Type {
        INSERT, UPDATE, DELETE        
    }
    
    public long serverId();
    
    public String databaseName();
    
    public String tableName();
    
    public Type type();

}
