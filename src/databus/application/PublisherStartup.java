package databus.application;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.listener.BatchListener;
import databus.network.Publisher;

public class PublisherStartup {

    public static void main(String[] args) {
        log.info("******************************************************************************");
        log.info("PublisherStartup will begin!");
        
        String configFileName = "conf/publisher.xml";
        if (args.length > 0) {
            configFileName = args[0];
        } 
        DatabusBuilder builder = new DatabusBuilder(configFileName);  
     
        Publisher publisher = builder.createPublisher();     
        BatchListener listener = builder.createListeners();
        listener.setPublisher(publisher);
        listener.start();       
        
        try {
            listener.join();
        } catch (InterruptedException e) {
            log.info("Has been interrupted!");
        } finally {
            listener.stop();
        }       
        
        log.info("PublisherStartup has finished!");
        System.exit(0);
    }
    
    private static Log log = LogFactory.getLog(PublisherStartup.class);
}
