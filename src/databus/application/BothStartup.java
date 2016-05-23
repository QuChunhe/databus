package databus.application;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.listener.BatchListener;
import databus.network.Publisher;
import databus.network.Subscriber;

public class BothStartup extends Startup {

    public static void main(String[] args) throws InterruptedException {                
        log.info("******************************************************************************");
        log.info("BothStartup will begin!");
        
        savePid("data/pid");
        String configFileName = "conf/databus.xml";
        if (args.length > 0) {
            configFileName = args[0];
        }        
        DatabusBuilder builder = new DatabusBuilder(configFileName);
        Subscriber subscriber = builder.createSubscriber() ;
        Publisher publisher = builder.createPublisher();       
        BatchListener listeners = builder.createListeners();
 
        subscriber.start();
        listeners.setPublisher(publisher);         
        listeners.start();   
        
        try {
            subscriber.join();
            listeners.join();
        } catch (InterruptedException e) {
            log.info("Has been interrupted!");
        } finally {
            subscriber.stop();
            listeners.stop();
        }
        
        log.info("BothStartup has finished!");
        System.exit(0);
    }
    
    private static Log log = LogFactory.getLog(BothStartup.class);
}
