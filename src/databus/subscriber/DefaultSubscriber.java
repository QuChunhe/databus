package databus.subscriber;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Subscriber;

public class DefaultSubscriber implements Subscriber{

    @Override
    public boolean receive(Event event) {
        log.info(event.toString());        
        return true;
    }
    
    private Log log = LogFactory.getLog(DefaultSubscriber.class);

}
