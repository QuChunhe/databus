package databus.network;


import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import databus.core.Subscriber;

public abstract class AbstractSubscriber implements Subscriber {    

    public AbstractSubscriber() {
        receiversMap = new ConcurrentHashMap<String, Set<Receiver>>();
    }
    
    public AbstractSubscriber(Map<String, Set<Receiver>> receoversMap) {
        this.receiversMap = receoversMap;
    }

    @Override
    public void join() throws InterruptedException {
        thread.join();        
    }

    @Override
    public boolean isRunning() {
        return (null!=thread) && (thread.getState()!=Thread.State.TERMINATED);
    }

    @Override
    public void start() {
        if (null == thread) {
            thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                run0();               
                            }                
                         }, this.getClass().getSimpleName());
            thread.start();
        }         
    }

    @Override
    public void stop() {
        if ((null != thread) && (thread.isAlive())) {
            thread.interrupt();
        }        
    }

    @Override
    public void register(String topic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(topic);
        if (null == receiversSet) {
            receiversSet = new CopyOnWriteArraySet<Receiver>();
            receiversMap.put(topic, receiversSet);
        }
        receiversSet.add(receiver);        
    }
    
    public boolean receive(Event event) {
        return receive0(event.topic(), event);
    }
    
    protected boolean receive0(String topic, Event event) {
        Set<Receiver> receiversSet = receiversMap.get(topic);
        if ((null==receiversSet) || (receiversSet.size()==0)){
            log.error(topic + " has't been subscribed!");
            return false;
        } else {
            receive1(receiversSet, event);
        }
        return true;
    }
    
    private void receive1(Set<Receiver> receiversSet, Event event) {
        if (null == receiversSet) {
            return;
        }
        for (Receiver receiver : receiversSet) {
            try {
                receiver.receive(event);
            } catch (Exception e) {
                String className = receiver.getClass().getName();
                log.error(className+" can't receive "+ event.toString(), e);
            }
        }
    }
    
    protected abstract void run0();    
    
    protected Map<String, Set<Receiver>> receiversMap;
    
    private static Log log = LogFactory.getLog(AbstractSubscriber.class);
    
    private Thread thread = null;
}
