package databus.application;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Initializable;
import databus.core.Listener;
import databus.core.Receiver;
import databus.listener.BatchListener;
import databus.network.NettyPublisher;
import databus.network.Publisher;
import databus.network.Subscriber;


public class DatabusBuilder {
    
    public DatabusBuilder() {
        this("conf/databus.xml");
    }

    public DatabusBuilder(String configFile) {
        try {
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<XMLConfiguration> builder =
                    new FileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
                        .configure(params.xml()
                        .setFileName(configFile));

            config = builder.getConfiguration();
        } catch (ConfigurationException e) {
            log.error("Can't load "+configFile, e);
            System.exit(1);
        }
    }
    
    public Publisher createPublisher() {
        String className = config.getString("publisher.class").trim();
        if ((null==className) || (className.length()==0)) {
            log.error("pulisher.class is null!");
            System.exit(1);
        }

        Publisher publisher = null;
        try {
            publisher = (Publisher)Class.forName(className).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            log.error("Can't initiate Publisher class : "+className, e);
            System.exit(1);
        }
        Configuration pubConfig = config.configurationAt("publisher");
        publisher.initialize(ConfigurationConverter.getProperties(pubConfig));
        return publisher;
    }
    
    public Subscriber createSubscriber() { 
        String className = config.getString("subscriber.class").trim();
        if ((null==className) || (className.length()==0)) {
            log.error("subscriber.class is null!");
            System.exit(1);
        }
        Subscriber subscriber = null;
        try {
            subscriber = (Subscriber) Class.forName(className).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            log.error("Can't initiate Subscriber class : "+className, e);
            System.exit(1);
        }
        Configuration subConfig = config.configurationAt("subscriber");
        subscriber.initialize(ConfigurationConverter.getProperties(subConfig));
        loadReceivers(subscriber);
        return subscriber;
    }
    
    public BatchListener createListeners() {
        BatchListener batchListener = new BatchListener();
        List<HierarchicalConfiguration<ImmutableNode>> 
             listenersConfig = config.configurationsAt("publisher.listener");
        for(HierarchicalConfiguration<ImmutableNode> c : listenersConfig) {
            Object object = loadInitialiableObject(c);
            if ((null!=object) && (object instanceof Listener)) {
                batchListener.add((Listener) object);
            } else {
                log.error("Can't instance Listener Object for "+c.toString());
            }
        }
        return batchListener;
    }
    
    public void loadReceivers(Subscriber subscriber) {
        List<HierarchicalConfiguration<ImmutableNode>> 
            subscribersConfig = config.configurationsAt("subscriber.receiver");
        for(HierarchicalConfiguration<ImmutableNode> sc : subscribersConfig) {
            Object object = loadInitialiableObject(sc);
            if ((null!=object) && (object instanceof Receiver)) {
                Receiver receiver = (Receiver)object;
                String[] remoteTopics = sc.getStringArray("remoteTopic");                
                for(String t : remoteTopics) {
                    subscriber.register(t, receiver);                                    
                }
            } else {
                log.error("Can't instantiate "+sc.toString());
            }
        }
    }
    
    public void loadSubscribers(NettyPublisher publisher) {
        List<HierarchicalConfiguration<ImmutableNode>> 
             subscribersConfig = config.configurationsAt("publisher.subscriber");
        for(HierarchicalConfiguration<ImmutableNode> c : subscribersConfig) {
            String topic = c.getString("topic");
            String host = c.getString("host");
            int port = c.getInt("port");
            InetSocketAddress remoteAddress = new InetSocketAddress(host, port);
            publisher.subscribe(topic, remoteAddress);
        }
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

    
    private static Log log = LogFactory.getLog(DatabusBuilder.class);
        
    private XMLConfiguration config;
}
