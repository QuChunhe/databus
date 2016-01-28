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
import databus.util.CompleteTopic;
import databus.util.InternetAddress;
import databus.util.RemoteTopic;
import databus.util.Timer;


public class InteractiveSubscriber extends Subscriber {

    public InteractiveSubscriber(Client client) {
        super();
        this.client = client;
        timer = new Timer("SubscribingTimer");
        task = new Task();
        confirmedTimeMap = new ConcurrentHashMap<RemoteTopic,Long>();
    }
    
    @Override
    public void register(RemoteTopic remoteTopic, Receiver receiver) {
        if (remoteTopic instanceof CompleteTopic) {
            super.register(remoteTopic, receiver);
        } else {
            log.error("remoteTopic must be am instance of CompleteTopic");
        }
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
           log.warn("nosubscribed event:" + event.toString()); 
        }
        
        return hasReceived;
    }
    
    public void subscribe() {
        if (!timer.contains(task)) {
            schedulSubscription();
        }
    }
    
    public void subscribe(CompleteTopic remoteTopic) {
        InternetAddress remoteAddress = remoteTopic.internetAddress();
        String topic = remoteTopic.topic();
        if (!remoteAddress.isValid()) {
            log.error(remoteAddress.toString() + "isn't valid Internet address");
            return;
        }
        if ((null==topic) || (topic.length()==0)) {
            log.error("topic is null");
            return;
        }
        Subscription event = new Subscription();
        event.topic(topic);
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
            RemoteTopic remoteTopic = new RemoteTopic(subs.ipAddress(), subs.topic());
            confirmedTimeMap.put(remoteTopic, System.currentTimeMillis());
        } else {
            log.info("Have received ConfirmationEvent : "+event.toString());
        }
    }    

    public void remove(InternetAddress remoteAddress, String topic) {
        RemoteTopic remoteTopic = new RemoteTopic(remoteAddress.ipAddress(), topic);
        remove(remoteTopic);
        Withdrawal withdrawal = new Withdrawal();
        withdrawal.topic(topic);
        client.send(withdrawal, remoteAddress);
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
                subscribe((CompleteTopic)remoteTopic);
            } else {
                interval = maxPeroidSec - (currentTime - confirmedTime);
            }
            if (interval < minInterval) {
                minInterval = interval;
            }
        }        
        timer.schedule(task, minInterval, TimeUnit.SECONDS);
    }

    private static Log log = LogFactory.getLog(InteractiveSubscriber.class);
    
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
