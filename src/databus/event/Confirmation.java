package databus.event;

import databus.core.Event;

public interface Confirmation<E extends Event> {
    
    public E getConfirmedEvent();
}
