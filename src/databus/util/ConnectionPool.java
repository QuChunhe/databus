package databus.util;

import java.sql.Connection;

public class ConnectionPool {
    
    public static ConnectionPool instance() {
        return instance;
    }
    
    public Connection getConnection(){
        return null;
    }
    
    
    private ConnectionPool() {        
    }
    
    private static ConnectionPool instance = new ConnectionPool();

}
