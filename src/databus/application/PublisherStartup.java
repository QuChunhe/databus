package databus.application;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Listener;
import databus.core.Publisher;

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
      
        List<Listener> listeners = builder.createListeners(publisher);     
        for(Listener l : listeners) {
           addShutdownHook(l); 
        }
        addShutdownHook(publisher); //add publisher after listeners
        waitUntilSIGTERM();        
        
        log.info("PublisherStartup has finished!");
        log.info("******************************************************************************");
    }
    
    private static Log log = LogFactory.getLog(PublisherStartup.class);
}
