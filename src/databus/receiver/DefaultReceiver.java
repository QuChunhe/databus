package databus.receiver;

import java.io.IOException;

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
    public void close() throws IOException {
    }

    private final static Log log = LogFactory.getLog(DefaultReceiver.class);
}
