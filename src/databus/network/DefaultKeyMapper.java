package databus.network;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import databus.core.Event;

/**
 * Created by Qu Chunhe on 2018-05-31.
 */
public class DefaultKeyMapper implements KeyMapper {

    public DefaultKeyMapper() {
        eventClasses = new HashMap<>();
        ServiceLoader<Event> serviceLoader = ServiceLoader.load(Event.class);
        for(Event e : serviceLoader) {
            Class<? extends Event> C = e.getClass();
            eventClasses.put(e.source().toString()+":"+e.type(), C);
        }
    }

    @Override
    public String toKey(Event event) {
        return event.source().toString()+":"+event.type();
    }

    @Override
    public Class<? extends Event> toEventClass(String key) {
        return eventClasses.get(key);
    }

    private final Map<String,Class<? extends Event>> eventClasses;
}
