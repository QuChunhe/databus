package databus.network;

import java.text.DateFormat;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import databus.core.Event;

public class JsonEventParser implements EventParser { 
    
    public JsonEventParser() {
        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .setDateFormat(DateFormat.LONG)                              
                                .create();        
    }
    
    @Override
    public String toMessage(Event event) {
        StringBuilder builder = new StringBuilder(2048);
        gson.toJson(event, builder);
        return builder.toString();
    }
    
    @Override
    public Event toEvent(String topic, String key, String message) {
        if (null == message) {
            log.error("Received value is null!");
            return null;
        }
        if (null != topic) {
            Event event = toEvent0(topic, message);
            if (null != event) {
                return event;
            }
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
            log.error(message + " can not be split by '='!");
            return null;
        }

        String key = parts[0].trim();
        String data = parts[1].trim();
        return toEvent0(key, data);
    }

    @Override
    public String toKey(Event event) {
        return keyMapper.toKey(event);
    }

    public void setKeyMapper(KeyMapper keyMapper) {
        this.keyMapper = keyMapper;
    }

    private Event toEvent0(String key, String data) {
        Class<? extends Event> eventClass = keyMapper.toEventClass(key);
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
    private final Pattern SPLIT_PATTERN = Pattern.compile("=");

    private KeyMapper keyMapper = new DefaultKeyMapper();
}
