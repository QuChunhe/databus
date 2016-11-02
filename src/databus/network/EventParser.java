package databus.network;

import databus.core.Event;

public interface EventParser {
    
    String toString(Event event);
    
    Event toEvent(String message);
}
