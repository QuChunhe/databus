package databus.network;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
import databus.util.InetTopic;

public class Subscriber {

    public Subscriber() {
        receiversMap = new ConcurrentHashMap<InetTopic, Set<Receiver>>();
    }

    public boolean receive(Event event) {
        InetTopic remoteTopic = new InetTopic(event.ipAddress(), event.topic());
        Set<Receiver> receiversSet = receiversMap.get(remoteTopic);
        if (null == receiversSet) {
            log.warn(remoteTopic.toString() + " has't been subscribed!");
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

    public void register(InetTopic remoteTopic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(remoteTopic);
        if (null == receiversSet) {
            receiversSet = new CopyOnWriteArraySet<Receiver>();
            receiversMap.put(remoteTopic, receiversSet);
        }
        receiversSet.add(receiver);
    }

    public void withdraw(InetTopic remoteTopic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(remoteTopic);
        if (null == receiversSet) {
            log.error(
                    "Don't contain the RemoteTopic " + remoteTopic.toString());
        } else {
            if (!receiversSet.remove(receiver)) {
                log.error("Don't contain the receiver " + receiver.toString());
            } else if (receiversSet.size() == 0) {
                remove(remoteTopic);
            }
        }
    }

    protected void remove(InetTopic remoteTopic) {
        Set<Receiver> receivers = receiversMap.get(remoteTopic);
        if (null != receivers) {
            receivers.clear();
        }
        receiversMap.remove(remoteTopic);
    }

    protected Map<InetTopic, Set<Receiver>> receiversMap;

    private static Log log = LogFactory.getLog(Subscriber.class);
}
