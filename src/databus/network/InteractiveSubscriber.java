package databus.network;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
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
import databus.util.SocketTopic;
import databus.util.InetTopic;
import databus.util.Timer;


public class InteractiveSubscriber extends Subscriber {

    public InteractiveSubscriber(Client client) {
        super();
        this.client = client;
        timer = new Timer("SubscribingTimer");
        task = new Task();
        confirmedTimeMap = new ConcurrentHashMap<InetTopic,Long>();
    }
    
    @Override
    public void register(InetTopic remoteTopic, Receiver receiver) {
        if (remoteTopic instanceof SocketTopic) {
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
    
    public void subscribe(SocketTopic remoteTopic) {
        SocketAddress remoteAddress = remoteTopic.socketAddress();
        String topic = remoteTopic.topic();
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
            InetTopic remoteTopic = new InetTopic(subs.ipAddress(), subs.topic());
            confirmedTimeMap.put(remoteTopic, System.currentTimeMillis());
        } else {
            log.info("Have received ConfirmationEvent : "+event.toString());
        }
    }    

    public void remove(InetSocketAddress remoteAddress, String topic) {
        InetTopic remoteTopic = new InetTopic(remoteAddress.getAddress(), topic);
        remove(remoteTopic);
        Withdrawal withdrawal = new Withdrawal();
        withdrawal.topic(topic);
        client.send(withdrawal, remoteAddress);
    }
    
    private void schedulSubscription() {
        Set<InetTopic> subscribedTopics = receiversMap.keySet();
        for(InetTopic topic : confirmedTimeMap.keySet()) {
            if (!subscribedTopics.contains(topic)) {
                confirmedTimeMap.remove(topic);
            }
        }
        
        long minInterval = maxPeroidSec;
        log.info(confirmedTimeMap.toString());
        for(InetTopic remoteTopic : subscribedTopics) {
            Long confirmedTimeObject = confirmedTimeMap.get(remoteTopic);
            if (null == confirmedTimeObject) {
                confirmedTimeObject = 0L;
            }
            long confirmedTime = confirmedTimeObject.longValue();
            long interval = minPeroidSec;
            long currentTime = System.currentTimeMillis()/1000L;
            if ((currentTime - confirmedTime) >= maxPeroidSec) {
                subscribe((SocketTopic)remoteTopic);
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
    private Map<InetTopic, Long> confirmedTimeMap = null;    
    
    private class Task implements Runnable {
        public Task() {
        }

        @Override
        public void run() {
            schedulSubscription();          
        }
    }
}
