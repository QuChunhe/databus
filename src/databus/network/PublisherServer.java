package databus.network;

import java.text.DateFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import databus.core.Event;
import databus.core.Publisher;

public class PublisherServer implements Publisher{   
    
    public PublisherServer() {
        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .serializeNulls()   
                                .setDateFormat(DateFormat.LONG)
                                .create();
        subscribers = new ConcurrentHashMap<String,List<InternetAddress>>(); 
    }

    public void subscribe(String topic, String ipAddress, int port) {
        
        
    }
    
    public void unsubscribe(String topic, String ipAddress, int port) {
        
    }

    @Override
    public void publish(Event event) {        
        
    }
    
    private static Log log = LogFactory.getLog(PublisherServer.class);
    
    private Map<String,List<InternetAddress>> subscribers;
    private Gson gson;
    
}
