package databus.core;

import java.io.Closeable;

public interface Subscriber extends Initializable, Joinable, Startable, Stoppable , Closeable {
    
    public void register(String topic, Receiver receiver);
}
