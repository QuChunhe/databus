package databus.network;

import databus.core.Event;

public interface EventParser {
    
    String toMessage(Event event);

    String toKey(Event event);
    
    Event toEvent(String key, String message);
}
