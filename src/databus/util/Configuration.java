package databus.util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Subscriber;

public class Configuration {
    
    public String TABLE_FILE_NAME = "conf/tables.properties";
    
    public String SERVER_CONFIGURATION_NAME = "conf/server.properties";
    
    public String SUBSCRIBING_CONFIGURATION_NAME = "conf/subscribers.properties";
    
    public static Configuration instance() {
        return instance;
    }
   
    public Properties loadSubscribersProperties() {
        String confFileName = SUBSCRIBING_CONFIGURATION_NAME;
        Properties properties= new Properties();
        try {
            properties.load(new FileReader(confFileName));                      
        } catch (FileNotFoundException e) {
            log.error("Cannot find "+confFileName, e);
            return null;
        } catch (IOException e) {
            log.error("cannot read "+confFileName, e);
            return null;
        }
        return properties;
    }
    
    public RemoteTopic parseRemoteTopic(String rawString) {
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
    
    public Set<Subscriber> parseSubscribers(String rawString) {
        Set<Subscriber> subscribers = new HashSet<Subscriber>();
        String[] classNames = rawString.split(",");
        for(String aClassName : classNames) {
            try {
                String aName = aClassName.trim();
                Class<? extends Subscriber> subscriberClass = 
                             Class.forName(aName).asSubclass(Subscriber.class);
                Subscriber subscriber = subscriberClass.newInstance();
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

}
