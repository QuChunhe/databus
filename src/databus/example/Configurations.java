package databus.example;

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
import databus.listener.BatchListener;
import databus.network.Subscriber;
import databus.util.InternetAddress;

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
        
    }
    
    public BatchListener loadListeners() {
        BatchListener batchListener = new BatchListener();
        List<HierarchicalConfiguration> 
             listenersConfig = config.configurationsAt("publisher.listeners");
        for(HierarchicalConfiguration c : listenersConfig) {
            Object object = loadInitialiableObject(c);
            if ((null!=object) && (object instanceof  Listener)) {
                batchListener.add((Listener) object);
            } else {
                log.error("Can't instance Listener Object for "+c.toString());
            }
        }
        return batchListener;
    }
    
    private Object loadInitialiableObject(Configuration c) {
        String className = c.getString("class");
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
    
    private static Log log = LogFactory.getLog(Configurations.class);
        
    private XMLConfiguration config;
}
