package databus.receiver;

import java.util.Properties;

import databus.core.Event;
import databus.core.Receiver;
import databus.event.mysql.MysqlInsertEvent;

public class MysqlReplication implements Receiver{

    @Override
    public void receive(Event event) {
        if (event instanceof MysqlInsertEvent) {
            
        }
    }
    
    
    
    @Override
    public void initialize(Properties properties) {
        // TODO Auto-generated method stub
        
    }



    private boolean insert(MysqlInsertEvent event) {
        return false;
    }
    

}
