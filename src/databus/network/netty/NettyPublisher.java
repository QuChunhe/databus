package databus.network.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.network.AbstractPublisher;

public class NettyPublisher extends AbstractPublisher {

    public NettyPublisher() {
        subscribersMap = new ConcurrentHashMap<String, Set<SocketAddress>>();
    }
    

    public void initialize(Properties properties) {        
        int threadPoolSize = Integer.parseInt(properties.getProperty("netty.threadPoolSize")); 
        int connectionsPerThread 
                  = Integer.parseInt(properties.getProperty("netty.connectionsPerThread"));
        int connectingListenersPerThread 
                  = Integer.parseInt(properties.getProperty("netty.connectingListenersPerThread"));
        
        client = new NettyClient(threadPoolSize, 
                                 connectionsPerThread, 
                                 connectingListenersPerThread);        
        
        String rawTopics = properties.getProperty("netty.subscriber.topic");
        String rawHosts = properties.getProperty("netty.subscriber.host");
        if ((null == rawTopics) || (null == rawHosts)) {
            log.error("Subscribers are null!");
            System.exit(1);
        }
        String[] topics = rawTopics.split(",");
        String[] hosts = rawHosts.split(",");
        
        if (topics.length != hosts.length) {
            log.error("topics are not consistent with hosts");
            System.exit(1);
        }
        for(int i=0; i<topics.length; i++) {
            String[] parts = hosts[i].split(":");
            if (parts.length != 2) {
                log.error(hosts[i] + " is illegal!");
                System.exit(1);
            }
            String addr = parts[0].trim();
            int port = Integer.parseInt(parts[1]);
            InetSocketAddress remoteAddress = new InetSocketAddress(addr, port);
            subscribe(topics[i].trim(), remoteAddress);
        }
                
    }
    
    @Override
    public void publish(Event event) {
        String topic = event.topic();
        Set<SocketAddress> addresses = subscribersMap.get(topic);
        if (null != addresses) {
            client.send(event, addresses);
        } else {
            log.info(event.toString()+" has't any subscriber!");
        }
    }

    @Override
    public void stop() {
        client.stop();        
    }
    
    public boolean subscribe(String topic, SocketAddress subscriber) {
        boolean hasAdded = false;
        Set<SocketAddress> addresses = subscribersMap.get(topic);
        if (null == addresses) {
            addresses = new CopyOnWriteArraySet<SocketAddress>();            
            subscribersMap.put(topic, addresses);
        }
        if (addresses.contains(subscriber)){
            log.info(subscriber.toString()+" has been contained");
        } else {
            addresses.add(subscriber);
            hasAdded = true;
        }
        return hasAdded;
    } 


    protected Map<String, Set<SocketAddress>> subscribersMap;
    protected NettyClient client;

    private static Log log = LogFactory.getLog(NettyPublisher.class);
  
}
