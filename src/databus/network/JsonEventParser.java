package databus.network;

import java.text.DateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import databus.core.Event;

public class JsonEventParser implements EventParser { 
    
    public JsonEventParser() {
        eventClasses = new HashMap<String,Class<? extends Event>>();
        ServiceLoader<Event> serviceLoader = ServiceLoader.load(Event.class);
        for(Event e : serviceLoader) {
            Class<? extends Event> C = (Class<? extends Event>) e.getClass();
            eventClasses.put(e.source().toString()+":"+e.type(), C);
        }


        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .setDateFormat(DateFormat.LONG)                              
                                .create();        
    }
    
    @Override
    public String toString(Event event) {        
        StringBuilder builder = new StringBuilder(2048);
        gson.toJson(event, builder);
        return builder.toString();
    }
    
    @Override
    public Event toEvent(String key, String message) {
        if (null == message) {
            log.error("Received message is null!");
            return null;
        }
        if (null != key) {
            Event event = toEvent0(key, message);
            if (null != event) {
                return event;
            }
        }

        return toEventFromMessage(message);
    }

    public Event toEventFromMessage(String message) {
        String[] parts = SPLIT_PATTERN.split(message, 2);
        if (parts.length != 2) {
            log.error(message + " cannot be splitted by '='!");
            return null;
        }

        String key = parts[0].trim();
        String data = parts[1].trim();
        return toEvent(key, data);
    }

    private Event toEvent0(String key, String data) {
        Class<? extends Event> eventClass = eventClasses.get(key);
        if (null == eventClass) {
            return null;
        }

        Event event = null;
        try {
            event = gson.fromJson(data, eventClass);
        } catch (JsonSyntaxException e) {
            log.error(data+" can not convert to "+eventClass.getSimpleName(), e);
        }

        return event;
    }
    
    private final static Log log = LogFactory.getLog(JsonEventParser.class);
    
    private final Gson gson;
    private final Map<String,Class<? extends Event>> eventClasses;
    private final Pattern SPLIT_PATTERN = Pattern.compile("=");

}
