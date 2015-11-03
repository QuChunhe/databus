package databus.util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Receiver;
import databus.subscriber.AbstractSubscriber;

public class Configuration {
    
    public String SERVER_CONFIGURATION_NAME = "conf/server.properties";    
    public String SUBSCRIBING_CONFIGURATION_NAME = "conf/subscribers.properties";
    
    public static Configuration instance() {
        return instance;
    }    
    
    public Properties loadProperties(String fileName) {
        Properties properties= new Properties();
        Reader reader = null;
        try {
            reader = new FileReader(fileName);
            properties.load(reader);                      
        } catch (FileNotFoundException e) {
            log.error("Cannot find "+fileName, e);
        } catch (IOException e) {
            log.error("Cannot read "+fileName, e);
        } finally {
            if(null != reader) {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error("Cannot close "+fileName, e);
                }
            }
        }
        return properties;
    }
    
    public InternetAddress loadListeningAddress() {
        if (null== listeningAddress) {
            Properties properties = loadProperties(SERVER_CONFIGURATION_NAME);
            String ip = properties.getProperty("server.ip", "127.0.0.1");
            int port = Integer.parseInt(
                                properties.getProperty("server.port", "8765"));
            listeningAddress = new InternetAddress(ip, port); 
        }
        return listeningAddress;
    }
    
    /**
     * 
     * @return must be thread-safety
     */
    public Map<RemoteTopic, Set<Receiver>>  loadSubscribers() {
        Properties properties = loadProperties(SUBSCRIBING_CONFIGURATION_NAME);
        Map<RemoteTopic, Set<Receiver>> subscriberMap = 
                          new ConcurrentHashMap<RemoteTopic,Set<Receiver>>();
               
        for(Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString();
            RemoteTopic remoteTopic = parseRemoteTopic(key);
            if (null == remoteTopic) {
                log.error(key+" cannot be parsed as RemoteTopic!");
                continue;
            }
            
            String value = entry.getValue().toString();
            Collection<Receiver> subscribers = parseSubscribers(value);
            if((null == subscribers) || (subscribers.size()==0)) {
                log.error(value+" cannot be parsed as Subcribers");
                continue;
            }            
            for(Receiver s : subscribers) {
                if (s instanceof AbstractSubscriber) {
                    ((AbstractSubscriber) s).remoteTopic(remoteTopic);
                }
            }
            Set<Receiver> subscriberSet = 
                              new CopyOnWriteArraySet<Receiver>(subscribers);
            subscriberMap.put(remoteTopic, subscriberSet);
        }
        if (subscriberMap.size()==0) {
            
        }
        return subscriberMap;
    }
    
    private RemoteTopic parseRemoteTopic(String rawString) {
        String[] result = rawString.split("/",2);
        if(result.length != 2) {
            log.error(rawString+" cannot be splitted by '/'");
            return null;
        }
        
        String[] addressInfo = result[0].split(":");
        if (addressInfo.length != 2) {
            log.error(result[0]+" cannot be splitted by ':'");
            return  null;
        }
        int port = Integer.parseInt(addressInfo[1]);
        InternetAddress netAddress = new InternetAddress(addressInfo[0],port);
        
        String topic = result[1].replace('/', ':');
        return new RemoteTopic(netAddress, topic);
    }
    
    private Set<Receiver> parseSubscribers(String rawString) {
        Set<Receiver> subscribers = new HashSet<Receiver>();
        String[] classNames = rawString.split(",");
        for(String aClassName : classNames) {
            try {
                String aName = aClassName.trim();
                Class<? extends Receiver> subscriberClass = 
                             Class.forName(aName).asSubclass(Receiver.class);
                Receiver subscriber = subscriberClass.newInstance();
                subscribers.add(subscriber);
            } catch (ClassNotFoundException e) {
                log.error(aClassName+" isnot a class name", e);
            } catch (InstantiationException e) {
                log.error(aClassName+
                        " cannot be create by default constructor", e);
            } catch (IllegalAccessException e) {
                log.error(aClassName+
                        " cannot accesss the default constructor", e);
            }
        }
        return subscribers;
    }
     
    private Configuration() {
        
    }
    
    private static Log log = LogFactory.getLog(Configuration.class);
    private static Configuration instance = new Configuration();
    
    private InternetAddress listeningAddress = null;

}
