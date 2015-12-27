package databus.listener;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import databus.core.Listener;
import databus.network.Publisher;

public class BatchListener extends AbstractListener{    

    public BatchListener() {
        super();
        listeners = new LinkedList<Listener>();
    }

    @Override
    public void start() {
        for(Listener l : listeners) {
            l.start();
        }
    }

    @Override
    public boolean isRunning() {
        boolean isRunning = true;
        for(Listener l : listeners) {
            isRunning = isRunning && l.isRunning();
            if (!isRunning) {
                break;
            }
        }
        return isRunning;
    }

    @Override
    public void stop() {
        for(Listener l : listeners) {
            l.stop();
        }
        
    }
    
    public void add(Listener listener) {
        listeners.add(listener);
        if ((null != publisher) && (listener instanceof AbstractListener)) {
            ((AbstractListener)listener).setPublisher(publisher);
        }
    }

    @Override
    public void initialize(Properties properties) {
        
    }
    
    
    @Override
    public void setPublisher(Publisher publisher) {
        super.setPublisher(publisher);
        for(Listener l : listeners) {
            if (l instanceof AbstractListener) {
                ((AbstractListener)l).setPublisher(publisher);
            }
        }
    }



    private List<Listener> listeners;
}
