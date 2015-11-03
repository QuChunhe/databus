package databus.event;

import databus.core.Event;
import databus.network.Publisher;

public interface ManagementEvent extends Event{    
    public static enum Type {SUBSCRIPTION, COUNTERMAND}
    
    public void execute(Publisher publisher);

}
