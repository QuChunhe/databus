package databus.network;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.event.Confirmation;
import databus.event.management.SubscribingConfirmation;
import databus.event.management.Subscription;
import databus.util.RemoteTopic;
import databus.util.Timer;

public class PeriodicSubscriber extends Subscriber{

    public PeriodicSubscriber(Client client) {
        super(client);
        timer = new Timer("SubscribingTimer");
        task = new Task();
        confirmedTimeMap = new ConcurrentHashMap<RemoteTopic,Long>();
    }

    @Override
    public void subscribe() {
        super.subscribe();
        if (!timer.contains(task)) {
            schedulSubscription();
        }       
    }
    
    public void setMaxSubscribingPeroid(long seconds) {
        if (seconds <= 0) {
            log.error("SubscribingPeroid must be great than 0");
            return;
        }
        subscribingPeroid = seconds*1000;
    }
    
    @Override
    protected void process(Confirmation<?> event) {
        if (event instanceof SubscribingConfirmation) {
            Subscription subs = ((SubscribingConfirmation)event).getConfirmedEvent();
            RemoteTopic remoteTopic = new RemoteTopic(subs.address(), subs.topic());
            confirmedTimeMap.put(remoteTopic, System.currentTimeMillis());
        }
    }
       
    private void schedulSubscription() {
        Set<RemoteTopic> subscribedTopics = receiversMap.keySet();
        for(RemoteTopic topic : confirmedTimeMap.keySet()) {
            if (!subscribedTopics.contains(topic)) {
                confirmedTimeMap.remove(topic);
            }
        }
        
        long minInterval = subscribingPeroid;
        log.info(confirmedTimeMap.toString());
        for(RemoteTopic remoteTopic : subscribedTopics) {
            Long confirmedTimeObject = confirmedTimeMap.get(remoteTopic);
            if (null == confirmedTimeObject) {
                confirmedTimeObject = 0L;
            }
            long confirmedTime = confirmedTimeObject.longValue();
            long interval = 1000;
            long currentTime = System.currentTimeMillis();
            if ((currentTime - confirmedTime) >= subscribingPeroid) {
                subscribe(remoteTopic);
            } else {
                interval = subscribingPeroid - (currentTime - confirmedTime);
            }
            if (interval < minInterval) {
                minInterval = interval;
            }
        }        
        timer.schedule(task, minInterval, TimeUnit.MILLISECONDS);
    }

    private static Log log = LogFactory.getLog(PeriodicSubscriber.class);
    
    private Timer timer = null;
    private long subscribingPeroid = 60L * 60L;
    private Task task = null;
    private Map<RemoteTopic, Long> confirmedTimeMap = null;
    
    private class Task implements Runnable {
        public Task() {
        }

        @Override
        public void run() {
            schedulSubscription();          
        }
    }
}
