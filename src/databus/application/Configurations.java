package databus.application;

import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Initializable;
import databus.core.Listener;
import databus.core.Receiver;
import databus.listener.BatchListener;
import databus.network.Publisher;
import databus.network.Subscriber;
import databus.util.InternetAddress;
import databus.util.RemoteTopic;


public class Configurations {
    
    public Configurations() {
        this("conf/databus.xml");
    }

    public Configurations(String configFile) {
        try {
            config = new XMLConfiguration(configFile);
        } catch (ConfigurationException e) {
            log.error("Can't load "+configFile, e);
            System.exit(1);
        }
    }
    
    public InternetAddress loadServerAddress() {
        String host = config.getString("server.host", "127.0.0.1");
        int port = config.getInt("server.port", 8765);
        return new InternetAddress(host, port);
    }
    
    public void loadReceivers(Subscriber subscriber) {
        List<HierarchicalConfiguration> 
            subscribersConfig = config.configurationsAt("subscriber.receiver");
        for(HierarchicalConfiguration sc : subscribersConfig) {
            Object object = loadInitialiableObject(sc);
            if ((null!=object) && (object instanceof Receiver)) {
                Receiver receiver = (Receiver)object;
                List<HierarchicalConfiguration> 
                             topicsConfig = sc.configurationsAt("remoteTopic");                
                for(HierarchicalConfiguration rc : topicsConfig) {
                    String topic = normalizeTopic(rc.getString("topic"));
                    String host = rc.getString("host");
                    int port = rc.getInt("port");
                    RemoteTopic remoteTopic = new RemoteTopic(host, port, topic);
                    subscriber.register(remoteTopic, receiver); 
                }
                
            } else {
                log.error("Can't instantiate "+sc.toString());
            }
        }
    }
    
    public void loadSubscribers(Publisher publisher) {
        List<HierarchicalConfiguration> 
             subscribersConfig = config.configurationsAt("publisher.subscriber");
        for(HierarchicalConfiguration c : subscribersConfig) {
            String topic = normalizeTopic(c.getString("topic"));
            String host = c.getString("host");
            int port = c.getInt("port");
            InternetAddress remoteAddress = new InternetAddress(host, port);
            publisher.subscribe(topic, remoteAddress);
        }
    }
    
    public BatchListener loadListeners() {
        BatchListener batchListener = new BatchListener();
        List<HierarchicalConfiguration> 
             listenersConfig = config.configurationsAt("publisher.listener");
        for(HierarchicalConfiguration c : listenersConfig) {
            Object object = loadInitialiableObject(c);
            if ((null!=object) && (object instanceof Listener)) {
                batchListener.add((Listener) object);
            } else {
                log.error("Can't instance Listener Object for "+c.toString());
            }
        }
        return batchListener;
    }
    
    private Object loadInitialiableObject(Configuration c) {
        String className = c.getString("class");
        if (null == className) {
            log.error("Can't find the value of class in configuration file");
            return null;
        }
        Initializable instance = null;        
        try {
            instance = (Initializable) Class.forName(className).newInstance();
        } catch (InstantiationException e) {
            log.error("Can't instance "+className, e);
        } catch (IllegalAccessException e) {
            log.error("Can't access Class "+className, e);
        } catch (ClassNotFoundException e) {
            log.error("Can't not find "+className, e);
        }
        
        if (null != instance) {
            Properties properties = ConfigurationConverter.getProperties(c);
            instance.initialize(properties);
        }
        
        return instance;
    }    
    
    private String normalizeTopic(String topic) {
        topic = topic.startsWith("/") ? topic.substring(1) : topic;
        topic = topic.replace('/', ':');
        return topic;
    }
    
    private static Log log = LogFactory.getLog(Configurations.class);
        
    private XMLConfiguration config;
}
