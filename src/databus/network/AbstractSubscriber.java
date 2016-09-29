package databus.network;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import databus.core.Subscriber;
import databus.core.ThreadHolder;
import databus.util.Helper;
import databus.core.Runner;

public abstract class AbstractSubscriber  implements Subscriber {
    
    public AbstractSubscriber() {
        receiversMap = new ConcurrentHashMap<String, Set<Receiver>>();
    }    

    @Override
    public void initialize(Properties properties) {
        executor = Helper.loadExecutor(properties, 0);        
    }

    @Override
    public void join() throws InterruptedException {
        holder.join();               
    }

    @Override
    public void start() {
        holder = new ThreadHolder(createBackgroundRunners());
        holder.start();   
    }

    @Override
    public void stop() {
        if (null != holder) {
            holder.stop();
        } else {
            log.warn(getClass().getName() + " hasn't started!");
        }
        
        if ((null!=executor) && (!executor.isTerminated())) {
            log.info("Waiting ExecutorService termination!");
            try {
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Can't wait the terimination of ExecutorService", e);
            }            
        }
    }

    @Override
    public void close() throws IOException {
        for(Set<Receiver> receivers : receiversMap.values()) {
            for(Receiver r : receivers) {
                if (r instanceof Closeable) {
                    ((Closeable) r).close();
                }
            }
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
    
    protected abstract Runner[] createBackgroundRunners();
    
    private void receive(Set<Receiver> receiversSet, Event event) {
        if (null == receiversSet) {
            return;
        }
        for (Receiver receiver : receiversSet) {
            if (null != executor) {
                executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            receive0(receiver, event);                       
                        }
                    }
                );
            } else {
                receive0(receiver, event); 
            }            
        }
    }
    
    private void receive0(Receiver receiver, Event event) {
        try {
            receiver.receive(event);
        } catch (Exception e) {
            log.error(receiver.getClass().getName() + " can't receive " + event.toString(), e);
        }
    }
    
    protected Map<String, Set<Receiver>> receiversMap;
    
    private static Log log = LogFactory.getLog(AbstractSubscriber.class);  
    
    private ThreadHolder holder = null;
    private ExecutorService executor = null;

}
