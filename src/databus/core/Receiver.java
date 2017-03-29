package databus.core;

import java.io.Closeable;

public interface Receiver extends Closeable {
    
    void receive(Event event);

}
