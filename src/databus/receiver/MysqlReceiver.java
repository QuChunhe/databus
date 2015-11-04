package databus.receiver;

import java.sql.Connection;

import databus.core.Receiver;

public abstract class MysqlReceiver implements Receiver{
    
    protected Connection getConnection() {
        
        
        return null;
    }

}
