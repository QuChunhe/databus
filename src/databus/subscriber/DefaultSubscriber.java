package databus.subscriber;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;

public class DefaultSubscriber implements Receiver{

    @Override
    public boolean receive(Event event) {
        log.info(event.toString());        
        return true;
    }
    
    private Log log = LogFactory.getLog(DefaultSubscriber.class);

}
