package databus.network;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import databus.event.ManagementEvent;
import databus.event.management.SubscribingConfirmation;
import databus.event.management.Subscription;
import databus.event.management.Withdrawal;
import databus.util.InternetAddress;
import databus.util.RemoteTopic;
import databus.util.Timer;

public class Subscriber {

    public Subscriber(Client client) {
        receiversMap = new ConcurrentHashMap<RemoteTopic,Set<Receiver>>();
        this.client = client;
    }

    public void receive(Event event) {
        if (event instanceof ManagementEvent) {
            return;
        } else if (event instanceof SubscribingConfirmation) {
            confirmSubscription(((SubscribingConfirmation)event).getConfirmedEvent());
        } else {
            receive0(event);
        }
    } 

    /**
     * invoked by a single thread
     */
    public void subscribe() {
        synchronized(this) {
            if (null == timer) {
                timer = new Timer("SubscribingTimer");
            } 
            if (null == subscribingTask) {
                subscribingTask = new SubscribingTask();
            }
            if (null == confirmedTimeMap) {
                confirmedTimeMap = new ConcurrentHashMap<RemoteTopic,Long>();
            }
        }
        subscribe0();
    }
    
    public void register(RemoteTopic remoteTopic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(remoteTopic);
        if (null == receiversSet) {
            receiversSet = new CopyOnWriteArraySet<Receiver>();
            receiversMap.put(remoteTopic, receiversSet);
        }
        receiversSet.add(receiver);
    }
    
    public void register(String rawString, Receiver receiver) {
        String[] rawParts = rawString.split("/",2);
        if(rawParts.length != 2) {
            log.error(rawString+" can't be splitted by '/'");
            return;
        }
        
        String[] addressInfo = rawParts[0].split(":");
        if (addressInfo.length != 2) {
            log.error(rawParts[0]+" can't be splitted by ':'");
            return;
        }
        int port = Integer.parseInt(addressInfo[1]);
        InternetAddress netAddress = new InternetAddress(addressInfo[0],port);
        String topic = rawParts[1].replace("/", ":");
        RemoteTopic remoteTopic = new RemoteTopic(netAddress, topic);
        register(remoteTopic, receiver);
    }
    
    public void withdraw(RemoteTopic remoteTopic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(remoteTopic);
        if (null == receiversSet) {
           log.error("Don't contain the RemoteTopic "+remoteTopic.toString());
        } else {
            if (!receiversSet.remove(receiver)) {
                log.error("Don't contain the receiver "+receiver.toString());
            } else if (receiversSet.size()==0) {
                remove(remoteTopic);
            }
        }
    }
    
    public void setMaxSubscribingPeroid(long period, TimeUnit timeUnit) {
        if (period > 0) {
            subscribingPeroid = TimeUnit.SECONDS.convert(period, timeUnit);
            log.info(subscribingPeroid);
        } else {
            subscribingPeroid = -1;
            synchronized(this) {
                if (null != timer) {
                    timer.cancel();
                    timer = null;
                }
                subscribingTask = null;
                if (null != confirmedTimeMap) {
                    confirmedTimeMap.clear();
                    confirmedTimeMap = null;
                }
            }
        }
    }
    
    public void confirmSubscription(Subscription event) {
        RemoteTopic remoteTopic = new RemoteTopic(event.address(), event.topic());
        if (null == confirmedTimeMap) {
            log.error("Does't user subscribe function");
            return;
        }
        
        if (receiversMap.get(remoteTopic) == null) {
            log.error(remoteTopic.topic()+" has't receivers");
            return;
        }
        confirmedTimeMap.put(remoteTopic, System.currentTimeMillis()/1000);
    }
       
    private void subscribe0() {
        if (subscribingPeroid <= 0) {
            return;
        }
        
        long minInterval = subscribingPeroid;
        log.info(confirmedTimeMap.toString());
        for(RemoteTopic remoteTopic : receiversMap.keySet()) {
            Long confirmedTimeObject = confirmedTimeMap.get(remoteTopic);
            if (null == confirmedTimeObject) {
                confirmedTimeObject = 0L;
            }
            long confirmedTime = confirmedTimeObject.longValue();
            long interval = 1;
            long currentTime = System.currentTimeMillis()/1000;
            if ((currentTime - confirmedTime) >= subscribingPeroid) {
                subscribe(remoteTopic);
            } else {
                interval = subscribingPeroid - (currentTime - confirmedTime);
            }
            if (interval < minInterval) {
                minInterval = interval;
            }
        }        
        timer.schedule(subscribingTask, minInterval, TimeUnit.SECONDS);
    }
    
    private void subscribe(RemoteTopic remoteTopic) {
        InternetAddress remoteAddress = remoteTopic.remoteAddress();
        Subscription event = new Subscription();
        event.topic(remoteTopic.topic());
        client.send(event, remoteAddress);
    }
    
    private void remove(RemoteTopic remoteTopic) {
        Set<Receiver> receivers = receiversMap.get(remoteTopic);
        if (null != receivers) {
            receivers.clear();
        }
        receiversMap.remove(remoteTopic);
        if (null != confirmedTimeMap) {
            confirmedTimeMap.remove(remoteTopic);
        }
    }
    
    public void receive0(Event event) {
        RemoteTopic remoteTopic = new RemoteTopic(event.address(), event.topic());
        Set<Receiver> receiversSet = receiversMap.get(remoteTopic);
        if (null == receiversSet) {
            log.error(remoteTopic.toString() + " has't been subscribed!");
            Withdrawal withdrawal = new Withdrawal();
            withdrawal.topic(event.topic());
            client.send(withdrawal, event.address());
        } else {
            for (Receiver receiver : receiversSet) {
                receiver.receive(event);
            }
        }
    } 

    private static Log log = LogFactory.getLog(Subscriber.class);

    private Map<RemoteTopic, Set<Receiver>> receiversMap;
    private Client client;
    
    private Timer timer = null;
    private long subscribingPeroid = 60L * 60L;
    private SubscribingTask subscribingTask = null;
    private Map<RemoteTopic, Long> confirmedTimeMap = null;

    
    private class SubscribingTask implements Runnable {
        public SubscribingTask() {
        }

        @Override
        public void run() {
            subscribe0();          
        }
    }
}
