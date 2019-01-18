package databus.network;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Listener;
import databus.core.Publisher;

/**
 * Created by Qu Chunhe on 2016-10-27.
 */
public abstract class AbstractPublisher implements Publisher {

    public AbstractPublisher() {
        listenerSet = new HashSet<>();
    }

    public AbstractPublisher(Collection<Listener> listeners) {
        this();
        listenerSet.addAll(listeners);
    }

    @Override
    public void addListener(Listener listener) {
        listenerSet.add(listener);
    }

    @Override
    public void stop() {
        for(Listener listener : listenerSet) {
            listener.stop();
        }
    }

    @Override
    public void join() throws InterruptedException {
        int count = 0;
        for(Listener listener : listenerSet) {
            try {
                listener.join();
            } catch (InterruptedException e) {
                log.error(listener.getClass().getName()+" has been interrupted!", e);
                count++;
            }
        }
        if (count > 0) {
            throw new InterruptedException(count+" listeners in total "+listenerSet.size()+
                                           " has been interrupted!");
        }
    }

    @Override
    public void start() {
        for(Listener l : listenerSet) {
            l.start();
        }
    }

    private final static Log log = LogFactory.getLog(AbstractPublisher.class);

    private final Set<Listener> listenerSet;

}
