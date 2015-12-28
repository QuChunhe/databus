package databus.network;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import databus.event.Confirmation;
import databus.event.management.SubscribingConfirmation;
import databus.event.management.Subscription;
import databus.event.management.Withdrawal;
import databus.util.InternetAddress;
import databus.util.RemoteTopic;
import databus.util.Timer;


public class ActiveSubscriber extends Subscriber {

    public ActiveSubscriber(Client client) {
        super();
        this.client = client;
        timer = new Timer("SubscribingTimer");
        task = new Task();
        confirmedTimeMap = new ConcurrentHashMap<RemoteTopic,Long>();
    }

    @Override
    public boolean receive(Event event) {
        if (event.source() == Event.Source.MANAGEMENT) {
            return false;
        }
        if (event.source() == Event.Source.CONFIRMATION) {
            process((Confirmation<?>)event);
            return true;
        }
        
        boolean hasReceived = super.receive(event);
        if (!hasReceived) {
           Withdrawal withdrawal = new Withdrawal();
           withdrawal.topic(event.topic());
           client.send(withdrawal, event.address());  
        }
        
        return hasReceived;
    }
    
    public void subscribe() {
        if (!timer.contains(task)) {
            schedulSubscription();
        }
    }
    
    public void subscribe(RemoteTopic remoteTopic) {
        InternetAddress remoteAddress = remoteTopic.remoteAddress();
        Subscription event = new Subscription();
        event.topic(remoteTopic.topic());
        client.send(event, remoteAddress);
    }    
  
    public void setMinPeroid(long minPeroidSec) {
        this.minPeroidSec = minPeroidSec;
    }

    public void setMaxPeroid(long maxPeroidSec) {
        this.maxPeroidSec = maxPeroidSec;
    }

    protected void process(Confirmation<?> event) {
        if (event instanceof SubscribingConfirmation) {
            Subscription subs = ((SubscribingConfirmation)event).getConfirmedEvent();
            RemoteTopic remoteTopic = new RemoteTopic(subs.address(), subs.topic());
            confirmedTimeMap.put(remoteTopic, System.currentTimeMillis());
        } else {
            log.info("Have received ConfirmationEvent : "+event.toString());
        }
    }
    
    @Override
    protected void remove(RemoteTopic remoteTopic) {
        super.remove(remoteTopic);
        Withdrawal withdrawal = new Withdrawal();
        withdrawal.topic(remoteTopic.topic());
        client.send(withdrawal, remoteTopic.remoteAddress());
    }
    
    private void schedulSubscription() {
        Set<RemoteTopic> subscribedTopics = receiversMap.keySet();
        for(RemoteTopic topic : confirmedTimeMap.keySet()) {
            if (!subscribedTopics.contains(topic)) {
                confirmedTimeMap.remove(topic);
            }
        }
        
        long minInterval = maxPeroidSec;
        log.info(confirmedTimeMap.toString());
        for(RemoteTopic remoteTopic : subscribedTopics) {
            Long confirmedTimeObject = confirmedTimeMap.get(remoteTopic);
            if (null == confirmedTimeObject) {
                confirmedTimeObject = 0L;
            }
            long confirmedTime = confirmedTimeObject.longValue();
            long interval = minPeroidSec;
            long currentTime = System.currentTimeMillis()/1000L;
            if ((currentTime - confirmedTime) >= maxPeroidSec) {
                subscribe(remoteTopic);
            } else {
                interval = maxPeroidSec - (currentTime - confirmedTime);
            }
            if (interval < minInterval) {
                minInterval = interval;
            }
        }        
        timer.schedule(task, minInterval, TimeUnit.SECONDS);
    }

    private static Log log = LogFactory.getLog(ActiveSubscriber.class);
    
    protected Client client;
    private Timer timer = null;
    private long minPeroidSec = 1L;
    private long maxPeroidSec = 60L * 60L;
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
