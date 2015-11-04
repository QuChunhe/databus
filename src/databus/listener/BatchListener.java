package databus.listener;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import databus.core.Listener;

public class BatchListener implements Listener{    

    public BatchListener() {
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
    }

    @Override
    public void initialize(Properties properties) {
        
    }
    
    private List<Listener> listeners;
}
