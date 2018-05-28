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

import databus.core.*;

public abstract class AbstractSubscriber extends RunnerHolder implements Subscriber {
    
    public AbstractSubscriber() {
        receiversMap = new ConcurrentHashMap<>();
    }


    @Override
    public void start() {
        for(Receiver receiver : getReceiverSet()) {
            if (receiver instanceof Startable) {
                ((Startable) receiver).start();
            }
        }

        setRunner(createTransporter());
        start();
    }

    @Override
    public void stop() {
        super.stop();
        
        if ((null!= executorService) && (!executorService.isTerminated())) {
            log.info("Waiting ExecutorService termination!");
            try {
                executorService.shutdown();
                executorService.awaitTermination(10, TimeUnit.SECONDS);
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

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public void setReceiversMap(Map<String, Collection<Receiver>> receiversMap) {
        for (Map.Entry entry : receiversMap.entrySet()) {
            String topic = (String) entry.getKey();
            for (Receiver receiver : (Collection<Receiver>) entry.getValue()) {
                register(topic, receiver);
            }
        }
    }

    @Override
    public void receive(String topic, Event event) {
        Set<Receiver> receiversSet = receiversMap.get(topic);
        if ((null==receiversSet) || (receiversSet.size()==0)){
            log.error(topic + " has't been subscribed!");
            return;
        } else {
            receive(receiversSet, event);
        }
    }

    protected abstract Transporter createTransporter();
    
    private void receive(Set<Receiver> receiversSet, final Event event) {
        if (null == receiversSet) {
            return;
        }
        for (Receiver receiver : receiversSet) {
            if (null != executorService) {
                executorService.execute(
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
    
    private ExecutorService executorService = null;
}
