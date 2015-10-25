package databus.network;

import java.text.DateFormat;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import databus.core.Event;
import databus.core.Publisher;

public class PublishingServer implements Publisher{   
    
    public PublishingServer(Client client) {
        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .serializeNulls()   
                                .setDateFormat(DateFormat.LONG)
                                .create();
        subscribers = new ConcurrentHashMap<String,Set<InternetAddress>>();
        this.client = client;
    }

    public void subscribe(String topic, String ipAddress, int port) {
        InternetAddress address = new InternetAddress(ipAddress, port);
        Set<InternetAddress> addressSet = subscribers.get(topic);
        if (null == addressSet) {
            addressSet = new CopyOnWriteArraySet<InternetAddress>();
            addressSet.add(address);
            subscribers.put(topic, addressSet);
        } else if (addressSet.contains(address)){
            log.info(address.toString()+" has subscribeed before");
        } else {
            addressSet.add(address);
        }        
    }
    
    public void unsubscribe(String topic, String ipAddress, int port) {
        InternetAddress address = new InternetAddress(ipAddress, port);
        Set<InternetAddress> addressSet = subscribers.get(topic);
        if (addressSet.remove(address)) {
            if (addressSet.isEmpty()) {
                subscribers.remove(topic);
            }
        }
    }

    @Override
    public void publish(Event event) {
        String topic = event.topic();
        Set<InternetAddress> addressSet = subscribers.get(topic);
        if (null != addressSet) {
            String message = event.source().toString()+":"+
                             event.type()+"="+gson.toJson(event);
            for(InternetAddress address : addressSet) {
                Task task = new Task(address, message);
                client.addTask(task);
            }
        }        
    }
    
    private static Log log = LogFactory.getLog(PublishingServer.class);
    
    private Map<String,Set<InternetAddress>> subscribers;
    private Gson gson;
    private Client client;    
}
