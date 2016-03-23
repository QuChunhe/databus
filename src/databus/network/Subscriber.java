package databus.network;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Joinable;
import databus.core.Receiver;
import databus.core.Startable;

public class Subscriber implements Joinable, Startable{

    public Subscriber() {
        receiversMap = new ConcurrentHashMap<String, Set<Receiver>>();
    }

    public boolean receive(Event event) {
        Set<Receiver> receiversSet = receiversMap.get(event.fullTopic());
        if (null == receiversSet) {
            receiversSet = receiversMap.get(event.topic());
        }
        if (null == receiversSet) {
            log.warn(event.fullTopic() + " has't been subscribed!");
            return false;
        } else {
            for (Receiver receiver : receiversSet) {
                try {
                    receiver.receive(event);
                } catch (Exception e) {
                    String className = receiver.getClass().getName();
                    log.error(className+" can't receive "+ event.toString(), e);
                }
            }
        }
        return true;
    }

    public void register(String topic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(topic);
        if (null == receiversSet) {
            receiversSet = new CopyOnWriteArraySet<Receiver>();
            receiversMap.put(topic, receiversSet);
        }
        receiversSet.add(receiver);
    }

    public void withdraw(String topic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(topic);
        if (null == receiversSet) {
            log.error(
                    "Don't contain the RemoteTopic " + topic.toString());
        } else {
            if (!receiversSet.remove(receiver)) {
                log.error("Don't contain the receiver " + receiver.toString());
            } else if (receiversSet.size() == 0) {
                remove(topic);
            }
        }
    }
    
    @Override
    public void join() throws InterruptedException {
        server.join();
    }
    
    public void stop() {
        server.stop();
    }   
    
    
    @Override
    public boolean isRunning() {
        return server.isRunning();
    }

    @Override
    public void start() {
        server.start();
    }

    protected void setServer(Server server) {
        this.server = server;
    }

    protected void remove(String topic) {
        Set<Receiver> receivers = receiversMap.get(topic);
        if (null != receivers) {
            receivers.clear();
        }
        receiversMap.remove(topic);
    }

    protected Map<String, Set<Receiver>> receiversMap;

    private static Log log = LogFactory.getLog(Subscriber.class);
    
    private Server server;
}
