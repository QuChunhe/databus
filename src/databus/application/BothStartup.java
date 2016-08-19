package databus.application;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Listener;
import databus.core.Publisher;
import databus.core.Subscriber;

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
        subscriber.start();
        addShutdownHook(subscriber);
        Publisher publisher = builder.createPublisher();         
        List<Listener> listeners = builder.createListeners(publisher);
        for(Listener l : listeners) {
           addShutdownHook(l); 
        }
        addShutdownHook(publisher);
        waitUntilSIGTERM(); 
        
        log.info("BothStartup has finished!");
        log.info("******************************************************************************");
    }
    
    private static Log log = LogFactory.getLog(BothStartup.class);
}
