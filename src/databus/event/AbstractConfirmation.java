package databus.event;

import databus.core.Event;

public abstract class AbstractConfirmation<E extends Event> 
                                                   extends AbstractEvent
                                                   implements Confirmation<E> {

    @Override
    public E getConfirmedEvent() {
        // TODO Auto-generated method stub
        return confirmedEvent;
    }

    public void setConfirmedEvent(E e) {
        this.confirmedEvent = e;
    }

    @Override
    public Source source() {
        return Event.Source.CONFIRMATION;
    }

    @Override
    public String toString() {
        return "Confirm :" + confirmedEvent.toString();
    } 

    private E confirmedEvent = null;
    
}
