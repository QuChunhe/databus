package databus.application;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Subscriber;

public class SubscriberStartup extends Startup {

    public static void main(String[] args) { 
        log.info("******************************************************************************");
        log.info("SubscriberStartup will begin!");
        
        savePid("data/pid");
        
        String configFileName = "conf/subscriber.xml";
        if (args.length > 0) {
            configFileName = args[0];
        }         
        DatabusBuilder builder = new DatabusBuilder(configFileName);
        Subscriber subscriber = builder.createSubscriber();
        subscriber.start();
        addShutdownHook(subscriber);        
        waitUntilSIGTERM(); 
        log.info("SubscriberStartup has finished!");
        log.info("******************************************************************************");
    }
    
    private static Log log = LogFactory.getLog(SubscriberStartup.class);
}
