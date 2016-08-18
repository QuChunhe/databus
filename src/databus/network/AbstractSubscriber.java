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
import databus.core.ThreadHolder;
import databus.core.Runner;

public abstract class AbstractSubscriber  implements Subscriber {

    public AbstractSubscriber(int runnerNumber) {
        receiversMap = new ConcurrentHashMap<String, Set<Receiver>>();
        if (runnerNumber < 1) {
            throw new IllegalArgumentException(runnerNumber + " thread is illegal");
        }  
        for (int i=0; i<runnerNumber; i++) {
            Runner runner = createBackgroundRunner();
            holder.add(runner, runner.getClass().getName()+"-"+i);
        }
    }   
    
    public AbstractSubscriber() {
        this(1);
    }

    @Override
    public void join() throws InterruptedException {
        holder.join();               
    }

    @Override
    public boolean isRunning() {
        return holder.isRunning();
    }

    @Override
    public void start() {      
        holder.start();   
    }

    @Override
    public void stop() {
        holder.stop();                
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
        return receive(event.topic(), event);
    }
    
    protected boolean receive(String topic, Event event) {
        Set<Receiver> receiversSet = receiversMap.get(topic);
        if ((null==receiversSet) || (receiversSet.size()==0)){
            log.error(topic + " has't been subscribed!");
            return false;
        } else {
            receive(receiversSet, event);
        }
        return true;
    }
    
    protected abstract Runner createBackgroundRunner();
    
    private void receive(Set<Receiver> receiversSet, Event event) {
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
    
    protected Map<String, Set<Receiver>> receiversMap;
    
    private static Log log = LogFactory.getLog(AbstractSubscriber.class);  
    
    private ThreadHolder holder = new ThreadHolder();

}
