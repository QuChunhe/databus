package databus.application;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Publisher;
import databus.listener.BatchListeners;

public class PublisherStartup extends Startup {

    public static void main(String[] args) {
        log.info("******************************************************************************");
        log.info("PublisherStartup will begin!");
        
        savePid("data/pid");
        
        String configFileName = "conf/publisher.xml";
        if (args.length > 0) {
            configFileName = args[0];
        } 
        DatabusBuilder builder = new DatabusBuilder(configFileName);  
     
        Publisher publisher = builder.createPublisher();     
        BatchListeners listener = builder.createListeners();
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
