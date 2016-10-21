package databus.application;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Listener;
import databus.core.Publisher;
import databus.core.Subscriber;
import org.apache.logging.log4j.LogManager;

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
            Subscriber subscriber = builder.createSubscriber() ;
            subscriber.start();
            addShutdownHook(subscriber);
        }
        if (builder.hasPublisher()) {
            Publisher publisher = builder.createPublisher();
            List<Listener> listeners = builder.createListeners(publisher);
            for (Listener l : listeners) {
                addShutdownHook(l);
            }
            addShutdownHook(publisher);
        }      
        
        waitUntilSIGTERM(); 
        
        log.info("Databus has finished!");
        log.info("******************************************************************************");
        LogFactory.releaseAll();
        try {
            Class clazz = Class.forName("org.apache.logging.log4j.LogManager",
                                        false,
                                        Thread.currentThread().getContextClassLoader());
            Method method = clazz.getMethod("shutdown", null);
            if (Modifier.isStatic(method.getModifiers())) {
                LogManager.shutdown();
            }
        } catch (ClassNotFoundException e) {
            System.out.println("log4j packet does't exist!");
            e.printStackTrace(System.out);
        } catch (NoSuchMethodException e) {
            System.out.println("version of log4j is less than 2.6!");
            e.printStackTrace(System.out);
        }
        System.out.println("******************************************************************************");
    }
    
    private static Log log = LogFactory.getLog(DatabusStartup.class);
}
