package databus.receiver;

import databus.core.Event;

public interface BeanParser {
    
    public Bean parse(Event event);
}
