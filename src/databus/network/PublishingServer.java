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
import databus.util.Configuration;
import databus.util.InternetAddress;

public class PublishingServer implements Publisher, Startable{   
    
    public PublishingServer() {
        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .serializeNulls()   
                                .setDateFormat(DateFormat.LONG)
                                .create();
        subscriberMap = new ConcurrentHashMap<String,Set<InternetAddress>>();
        client = new Client();
        listeningAddress = Configuration.instance().loadListeningAddress();
    }

    @Override
    public void subscribe(String topic, InternetAddress remoteAddress) {
        Set<InternetAddress> addressSet = subscriberMap.get(topic);
        if (null == addressSet) {
            addressSet = new CopyOnWriteArraySet<InternetAddress>();
            addressSet.add(remoteAddress);
            subscriberMap.put(topic, addressSet);
        } else if (addressSet.contains(remoteAddress)){
            log.info(remoteAddress.toString()+" has subscribeed before");
        } else {
            addressSet.add(remoteAddress);
        }        
    }
    
    @Override
    public void unsubscribe(String topic, InternetAddress remoteAddress) {
        Set<InternetAddress> addressSet = subscriberMap.get(topic);
        if (addressSet.remove(remoteAddress)) {
            if (addressSet.isEmpty()) {
                subscriberMap.remove(topic);
            }
        } else {
            log.error(remoteAddress.toString()+" hasnot subscribe "+topic);
        }
    }

    @Override
    public void publish(Event event) {
        log.info(event);
        String topic = event.topic();
        Set<InternetAddress> remoteAddressSet = subscriberMap.get(topic);
        if (null != remoteAddressSet) {
            String message = stringOf(event);
            client.send(message, remoteAddressSet);
        }
    }
    
    @Override
    public void publish(InternetAddress remoteAddress, Event event) {
        client.send(stringOf(event), remoteAddress);
    }
    
    @Override
    public Thread start() {        
        return client.start();
    }
    
    public boolean isRunning() {
        return client.isRunning();
    }
    
    public void stop() {
        if (client.isRunning()) {
           client.stop(); 
        }
    }
    
    private String stringOf(Event e) {
        e.address(listeningAddress);
        return e.source().toString()+":"+e.type()+"="+gson.toJson(e);
    }
    
    private static Log log = LogFactory.getLog(PublishingServer.class);
    
    private Map<String,Set<InternetAddress>> subscriberMap;
    private Gson gson;
    private Client client; 
    private InternetAddress listeningAddress;
}
