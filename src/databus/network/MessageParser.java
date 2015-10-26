package databus.network;

import java.text.DateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import databus.core.Event;

public class MessageParser {
    
    public MessageParser() {
        loadEventClass();
        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .serializeNulls()   
                                .setDateFormat(DateFormat.LONG)
                                .create();
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
        
        String key = result[0].trim();
        String data = result[1].trim();         
        Class<? extends Event> eventClass = eventClasses.get(key);
        if (null == eventClass) {
            log.error(key+" cannot map a Event Class!");
            return null;
        }
              
        
        Event event = null;        
        try {
            event = gson.fromJson(data, eventClass);
        } catch (JsonSyntaxException e) {
            log.error(data+" can not convert to "+eventClass.getSimpleName(),e);
        }
        
        return event;
    }
    
    private void loadEventClass() {
        eventClasses = new HashMap<String,Class<? extends Event>>();
        ServiceLoader<Event> serviceLoader = ServiceLoader.load(Event.class);
        for(Event e : serviceLoader) {
            Class<? extends Event> C = (Class<? extends Event>) e.getClass();
            eventClasses.put(e.source().toString()+":"+e.type(), C);
        }
    }

    private static Log log = LogFactory.getLog(MessageParser.class);
    private Map<String,Class<? extends Event>> eventClasses;
    private Gson gson;
}
