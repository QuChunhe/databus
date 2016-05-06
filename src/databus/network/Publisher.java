package databus.network;

import databus.core.Event;
import databus.core.Initializable;
import databus.core.Stoppable;

public interface Publisher extends Initializable, Stoppable {
    
    public void publish(Event event);
}
