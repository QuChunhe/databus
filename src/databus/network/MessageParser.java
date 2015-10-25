package databus.network;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;

public class MessageParser {
    
    public MessageParser() {
        eventClasses = new HashMap<String,Class<? extends Event>>();
        ServiceLoader<Event> serviceLoader = ServiceLoader.load(Event.class);
        for(Event e : serviceLoader) {
            Class<? extends Event> C = (Class<? extends Event>) e.getClass();
            eventClasses.put(e.source().toString()+":"+e.type(), C);
            System.out.println(C.getSimpleName());
        }
        System.out.println("*************");
    }
    
    public Event parse(String message) {        
        if (null == message) {
            log.error("Received message is null!");
            return null;
        }
        
        String[] result = message.split("=",2);
        if (result.length != 2) {
            log.error(message+" cannot be splitted by '='!");
            return null;
        }
        
        String type = result[0];
        String data = result[1];         
        
        
        
        Event event = null;
        return event;
    }

    private static Log log = LogFactory.getLog(MessageParser.class);
    private Map<String,Class<? extends Event>> eventClasses;
}
