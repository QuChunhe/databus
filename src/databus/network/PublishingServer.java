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
import databus.util.InternetAddress;

public class PublishingServer implements Publisher, Startable{   
    
    public PublishingServer() {
        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .serializeNulls()   
                                .setDateFormat(DateFormat.LONG)
                                .create();
        subscribers = new ConcurrentHashMap<String,Set<InternetAddress>>();
        client = new Client();
        client.start();
    }

    @Override
    public void subscribe(String topic, InternetAddress remoteAddress) {
        Set<InternetAddress> addressSet = subscribers.get(topic);
        if (null == addressSet) {
            addressSet = new CopyOnWriteArraySet<InternetAddress>();
            addressSet.add(remoteAddress);
            subscribers.put(topic, addressSet);
        } else if (addressSet.contains(remoteAddress)){
            log.info(remoteAddress.toString()+" has subscribeed before");
        } else {
            addressSet.add(remoteAddress);
        }        
    }
    
    @Override
    public void unsubscribe(String topic, InternetAddress remoteAddress) {
        Set<InternetAddress> addressSet = subscribers.get(topic);
        if (addressSet.remove(remoteAddress)) {
            if (addressSet.isEmpty()) {
                subscribers.remove(topic);
            }
        }
    }

    @Override
    public void publish(Event event) {
        String topic = event.topic();
        Set<InternetAddress> remoteAddressSet = subscribers.get(topic);
        if (null != remoteAddressSet) {
            String message = stringOf(event);
            for(InternetAddress address : remoteAddressSet) {
                releaseMessage(address, message);
            }
        }        
    }
    
    @Override
    public void publish(InternetAddress address, Event event) {
        releaseMessage(address,stringOf(event));
    }
    
    @Override
    public void start() {
        client.start();
        
    }

    @Override
    public boolean isRunning() {
        return client.isRunning();
    }

    @Override
    public void stop() {
        client.stop();        
    }  
    
    private void releaseMessage(InternetAddress remoteAddress, String message) {
        Task task = new Task(remoteAddress, message);
        client.addTask(task);
    }
    
    private String stringOf(Event e) {
        return e.source().toString()+":"+e.type()+"="+gson.toJson(e);
    }
    
    private static Log log = LogFactory.getLog(PublishingServer.class);
    
    private Map<String,Set<InternetAddress>> subscribers;
    private Gson gson;
    private Client client;
  
}
