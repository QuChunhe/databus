package databus.receiver;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;

public class DefaultReceiver implements Receiver{

    @Override
    public void receive(Event event) {
        log.info(event.toString());        
    }
    
    @Override
    public void initialize(Properties properties) {
        log.info(properties.toString());
    }

    private Log log = LogFactory.getLog(DefaultReceiver.class);
}
