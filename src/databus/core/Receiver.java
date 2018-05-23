package databus.core;

import java.io.Closeable;

public interface Receiver extends Closeable {
    
    void receive(final Event event);

}
