package databus.subscriber;

import databus.core.Event;
import databus.core.Receiver;
import databus.event.mysql.MysqlInsertEvent;

public class MysqlReplication implements Receiver{

    @Override
    public void receive(Event event) {
        if (event instanceof MysqlInsertEvent) {
            
        }
    }
    
    
    
    private boolean insert(MysqlInsertEvent event) {
        return false;
    }
    

}
