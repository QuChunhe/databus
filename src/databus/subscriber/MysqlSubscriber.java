package databus.subscriber;

import databus.core.Event;
import databus.core.Subscriber;
import databus.event.MysqlEvent;
import databus.event.MysqlEvent.Type;
import databus.event.MysqlWriteEvent;

public class MysqlSubscriber implements Subscriber{

    @Override
    public boolean receive(Event event) {
        if (event instanceof MysqlWriteEvent) {
            switch(((MysqlWriteEvent) event).mysqlType()) {
            case INSERT:
                
            }
        }
        return false;
    }

}
