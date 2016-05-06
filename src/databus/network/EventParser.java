package databus.network;

import databus.core.Event;

public interface EventParser {
    
    public String toString(Event event);
    
    public Event toEvent(String message);
}
