package databus.network;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.util.Helper;
import databus.core.*;

public abstract class AbstractSubscriber  implements Subscriber {
    
    public AbstractSubscriber() {
        receiversMap = new ConcurrentHashMap<>();
    }

    @Override
    public void join() throws InterruptedException {
        holder.join();               
    }

    @Override
    public void start() {
        if (coreThreadPoolSize > 0) {
            executor = Helper.loadExecutor(coreThreadPoolSize, maxThreadPoolSize,
                                           keepAliveSeconds, taskQueueCapacity);
        }
        for(Receiver receiver : getReceiverSet()) {
            if (receiver instanceof Startable) {
                ((Startable) receiver).start();
            }
        }

        holder = new RunnerHolder(createTransporters());
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
                log.error("Can't wait the termination of ExecutorService", e);
            }            
        }

        for(Receiver receiver : getReceiverSet()) {
            try {
                receiver.close();
            } catch (IOException e) {
                log.error("Can not close "+receiver.getClass().getName(), e);
            }
        }
    }

    @Override
    public void register(String topic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(topic);
        if (null == receiversSet) {
            receiversSet = new CopyOnWriteArraySet<>();
            receiversMap.put(topic, receiversSet);
        }
        receiversSet.add(receiver);        
    }

    public boolean receive(Event event) {
        return receive(event.topic(), event);
    }

    public void setCoreThreadPoolSize(int coreThreadPoolSize) {
        this.coreThreadPoolSize = coreThreadPoolSize;
    }

    public void setMaxThreadPoolSize(int maxThreadPoolSize) {
        this.maxThreadPoolSize = maxThreadPoolSize;
    }

    public void setKeepAliveSeconds(long keepAliveSeconds) {
        this.keepAliveSeconds = keepAliveSeconds;
    }

    public void setTaskQueueCapacity(int taskQueueCapacity) {
        this.taskQueueCapacity = taskQueueCapacity;
    }

    public void setReceiversMap(Map<String, Collection<Receiver>> receiversMap) {
        for (Map.Entry entry : receiversMap.entrySet()) {
            String topic = (String) entry.getKey();
            for (Receiver r : (Collection<Receiver>) entry.getValue()) {
                register(topic, r);
            }
        }
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

    protected abstract Transporter[] createTransporters();
    
    private void receive(Set<Receiver> receiversSet, final Event event) {
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

    private Set<Receiver> getReceiverSet() {
        HashSet<Receiver> receiverSet = new HashSet<>();
        for(Set<Receiver> receivers : receiversMap.values()) {
            receiverSet.addAll(receivers);
        }
        return receiverSet;
    }
    
    protected final Map<String, Set<Receiver>> receiversMap;
    
    private final static Log log = LogFactory.getLog(AbstractSubscriber.class);
    
    private RunnerHolder holder = null;
    private ExecutorService executor = null;
    private int coreThreadPoolSize = 0;
    private int maxThreadPoolSize = 5;
    private long keepAliveSeconds = 60;
    private int taskQueueCapacity = 2;
}
