package databus.event;

import databus.core.Event;

public interface MySQLEvent  extends Event{   
    
    static public enum Type {
        INSERT, UPDATE, DELETE        
    }
    
    public long getServerId();
    
    public String getDatabaseName();
    
    public String getTableName();
    
    public Type type();

}
