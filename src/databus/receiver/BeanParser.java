package databus.receiver;

import databus.core.Event;

public interface BeanParser {
    
    Bean parse(Event event);
}
