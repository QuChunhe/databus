package databus.application;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
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
import databus.network.Client;
import databus.network.Publisher;
import databus.network.Server;
import databus.network.Subscriber;
import databus.util.InetTopic;


public class DatabusBuilder {
    
    public DatabusBuilder() {
        this("conf/databus.xml");
    }

    public DatabusBuilder(String configFile) {
        try {
            config = new XMLConfiguration(configFile);
        } catch (ConfigurationException e) {
            log.error("Can't load "+configFile, e);
            System.exit(1);
        }
    }
    
    public Publisher createPublisher() {
        int threadPoolSize = getIntValue("publisher.client.threadPoolSize", 1); 
        int connectionsPerThread = getIntValue("publisher.client.connectionsPerThread", 10);
        int connectingListenersPerThread 
                          = getIntValue("publisher.client.connectingListenersPerThread", 256);
        
        Client client = new Client(threadPoolSize, 
                                   connectionsPerThread, 
                                   connectingListenersPerThread);
        Publisher publisher = new Publisher(client);
        loadSubscribers(publisher);
        return publisher;
    }
    
    public Subscriber createSubscriber() {
        Subscriber subscriber = new Subscriber();
        SocketAddress localAddress = serverAddress();
        Server server = new Server(localAddress, serverThreadPoolSize());
        server.setSubscriber(subscriber);
        loadReceivers(subscriber);
        return subscriber;
    }
    
    public BatchListener createListeners() {
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
    
    public SocketAddress serverAddress() {
        String host = config.getString("subscriber.server.host");
        int port = config.getInt("subscriber.server.port");
        return new InetSocketAddress(host, port);
    }
    
    public int serverThreadPoolSize() {
        return getIntValue("subscriber.server.threadPoolSize", 1);
    }
    
    public int clientThreadPoolSize() {
        return getIntValue("publisher.client.threadPoolSize", 1);
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
                    try {
                        InetAddress address = InetAddress.getByName(host);
                        InetTopic remoteTopic = new InetTopic(address, topic);
                        subscriber.register(remoteTopic, receiver); 
                    } catch (UnknownHostException e) {
                        log.error("Cann't resolved "+host, e);
                    }                    
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
            InetSocketAddress remoteAddress = new InetSocketAddress(host, port);
            publisher.subscribe(topic, remoteAddress);
        }
    }
    
    private int getIntValue(String key, int defaultValue) {
        String valueString = config.getString(key);
        int value = defaultValue;
        if (null != valueString) {           
            value = Integer.parseInt(valueString);
        } else {
            log.warn(key+" hasn't value");
        }
        return value;
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
    
    private static Log log = LogFactory.getLog(DatabusBuilder.class);
        
    private XMLConfiguration config;
}
