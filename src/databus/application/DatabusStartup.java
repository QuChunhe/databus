package databus.application;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Publisher;
import databus.core.Subscriber;

public class DatabusStartup extends Startup {

    public static void main(String[] args) throws InterruptedException {                
        log.info("******************************************************************************");
        log.info("Databus will begin!");
        
        savePid("data/pid");
        String configFileName = "conf/databus.xml";
        if (args.length > 0) {
            configFileName = args[0];
        }

        DatabusBuilder builder = new DatabusBuilder(configFileName);
        if (builder.hasSubscriber()) {
            Subscriber subscriber = builder.createSubscriber();
            addShutdownHook(subscriber);
            subscriber.start();
        }
        if (builder.hasPublisher()) {
            Publisher publisher = builder.createPublisher();
            addShutdownHook(publisher);
        }
        //help GC
        builder = null;
        waitUntilSIGTERM(); 
        
        log.info("Databus has finished!");
        log.info("******************************************************************************");
        //gracefully close log
        LogFactory.releaseAll();
    }
    
    private static Log log = LogFactory.getLog(DatabusStartup.class);
}
