package databus.core;

import java.util.List;

public interface MySQLEvent<T>  extends Event{   
    
    static public enum Type {
        INSERT, UPDATE, DELETE        
    }
    
    public String getIpAddress();
    
    public int getServerId();
    
    public String getDatabaseName();
    
    public String getTableName();
    
    public Type getType();
    
    public List<T> getRows();
}
