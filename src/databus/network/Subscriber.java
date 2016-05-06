package databus.network;

import databus.core.Initializable;
import databus.core.Joinable;
import databus.core.Receiver;
import databus.core.Startable;
import databus.core.Stoppable;

public interface Subscriber extends Initializable, Joinable, Startable, Stoppable {
    
    public void register(String topic, Receiver receiver);
}
