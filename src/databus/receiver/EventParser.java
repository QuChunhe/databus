package databus.receiver;

import databus.core.Event;

public interface EventParser {
    
    public Bean parse(Event event);
}
