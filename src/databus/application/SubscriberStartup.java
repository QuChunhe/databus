package databus.application;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.network.Subscriber;

public class SubscriberStartup {

    public static void main(String[] args) { 
        log.info("******************************************************************************");
        log.info("SubscriberStartup will begin!");
        
        String configFileName = "conf/subscriber.xml";
        if (args.length > 0) {
            configFileName = args[0];
        }         
        DatabusBuilder builder = new DatabusBuilder(configFileName);

        Subscriber subscriber = builder.createSubscriber();
        subscriber.start();

        try {
            subscriber.join();
        } catch (InterruptedException e) {
            log.info("Has been interrupted!");
        } finally {
            subscriber.stop();
        }
        log.info("SubscriberStartup has finished!");
        System.exit(0);
    }
    
    private static Log log = LogFactory.getLog(SubscriberStartup.class);
}
