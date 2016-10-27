package databus.network;

import databus.core.Listener;
import databus.core.Publisher;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Qu Chunhe on 10/27/16.
 */
public abstract class AbstractPublisher implements Publisher {

    public AbstractPublisher() {
        listenerSet = new HashSet<Listener>();
    }

    @Override
    public void addListener(Listener listener) {
        listenerSet.add(listener);
        listener.setPublisher(this);
    }

    @Override
    public void stop() {
        for(Listener listener : listenerSet) {
            listener.stop();
        }
    }

    @Override
    public void join() throws InterruptedException {
        for(Listener listener : listenerSet) {
            listener.join();
        }
    }

    private Set<Listener> listenerSet;

}
