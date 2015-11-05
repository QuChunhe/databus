package databus.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Initializable;
import databus.core.Listener;
import databus.core.Receiver;
import databus.listener.AbstractListener;
import databus.listener.BatchListener;
import databus.network.Publisher;
import databus.network.Subscriber;
import databus.util.InternetAddress;

public class Configuration {
    
    public static String RECEIVER_CLASS = "receiver.class";
    public static String RECEIVER_TOPIC = "receiver.topic";
    public static String LISTENER_CLASS = "listener.class";
    public static String SERVER_IP = "server.ip";
    public static String SERVER_PORT = "server.port";
    
    public String SERVER_CONFIGURATION_NAME = "conf/server.properties";    
    public String RECEIVERS_PROPERTIES_DIR_NAME = "conf/receivers";
    public String LISTENERS_PROPERTIES_DIR_NAME="conf/listeners";
    
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
            log.error("Can't find "+fileName, e);
        } catch (IOException e) {
            log.error("Can't read "+fileName, e);
        } finally {
            if(null != reader) {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error("Can't close "+fileName, e);
                }
            }
        }
        return properties;
    }
    
    public InternetAddress loadListeningAddress() {
        Properties properties = loadProperties(SERVER_CONFIGURATION_NAME);
        String ip = properties.getProperty(SERVER_IP, "127.0.0.1");
        int port = Integer.parseInt(properties.getProperty(SERVER_PORT, 
                                                           "8765"));
        InternetAddress listeningAddress = new InternetAddress(ip, port);        
        return listeningAddress;
    }
    
    public void loadReceivers(Subscriber subscriber) {
        String dirName = RECEIVERS_PROPERTIES_DIR_NAME;
        Map<String, Properties> propertiesMap = loadPropertiesFrom(dirName);
        if (null == propertiesMap) {
            log.error("Can't load Receivers");
            System.exit(1);
        }
        for(String fileName : propertiesMap.keySet()) {
            Properties properties = propertiesMap.get(fileName);
            String className = properties.getProperty(RECEIVER_CLASS);
            if (null == className) {
                log.error("Don't define Receiver Class in "+fileName);
                continue;
            }
            String topicString = properties.getProperty(RECEIVER_TOPIC);
            if (null == topicString) {
                log.error("Don't define 'topic' in "+fileName);
                continue;
            }
            
            Receiver receiver = (Receiver) loadObject(className, properties);
            subscriber.register(topicString, receiver);
        }
    } 
    
    public BatchListener loadListeners(Publisher publisher) {
        String dirName = LISTENERS_PROPERTIES_DIR_NAME;
        Map<String, Properties> propertiesMap = loadPropertiesFrom(dirName);
        if (null == propertiesMap) {
            log.error("Can't load Listeners");
            System.exit(1);
        }
        BatchListener batchListener = new BatchListener();
        for(String fileName : propertiesMap.keySet()) {
            Properties properties = propertiesMap.get(fileName);
            String className = properties.getProperty(LISTENER_CLASS);
            if (null == className) {
                log.error("Don't define Listener Class in "+fileName);
                continue;
            }
            Listener listener = (Listener)loadObject(className, properties);
            if (listener instanceof AbstractListener) {
                ((AbstractListener) listener).setPublisher(publisher);
            }
            batchListener.add(listener);
        }
        return batchListener;
    }
    
    public Map<String, Properties> loadPropertiesFrom(String dirName) {
        File dir = new File(dirName);
        if (!dir.isDirectory()) {
            log.error(dirName+" isn't a directory");
            return null;
        }
        String[] fileNames = dir.list();
        HashMap<String, Properties> 
                             propertiesMap = new HashMap<String, Properties>();
        for(String name : fileNames) {
            String fullName = dirName+"/"+name;
            propertiesMap.put(name,loadProperties(fullName));
        }
        return propertiesMap;
    }

    private Initializable loadObject(String className, Properties properties) {
        Class<? extends Initializable> aClass = null;
        Initializable aObject = null;
        try {              
            aClass = Class.forName(className).asSubclass(Initializable.class);
            aObject = aClass.newInstance();
            aObject.initialize(properties);       
        } catch (ClassNotFoundException e) {
            log.error(className+" don't define", e);
        } catch (InstantiationException e) {
            log.error("Can't instantiate "+className, e);
        } catch (IllegalAccessException e) {
            log.error("Can't acccess constructor of "+className, e);
        }
        return aObject;
    }
     
    private Configuration() {
        
    }
    
    private static Log log = LogFactory.getLog(Configuration.class);
    private static Configuration instance = new Configuration();

}
