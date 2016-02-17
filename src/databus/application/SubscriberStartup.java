package databus.application;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.network.Server;
import databus.network.Subscriber;
import databus.util.InternetAddress;

public class SubscriberStartup {

    public static void main(String[] args) { 
        log.info("******************************************************************************");
        log.info("SubscriberStartup will begin!");
        
        String configFileName = "conf/subscriber.xml";
        if (args.length > 0) {
            configFileName = args[0];
        }         
        Configurations config = new Configurations(configFileName);
        InternetAddress localAddress = config.serverAddress();
        Server server = new Server(localAddress);
        Subscriber subscriber = new Subscriber();
        server.setSubscriber(subscriber);        
        Thread serverThread = server.start(); 
        config.loadReceivers(subscriber);

        try {
            serverThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            server.stop();
        }
        log.info("SubscriberStartup has finished!");
        System.exit(0);
    }
    
    private static Log log = LogFactory.getLog(SubscriberStartup.class);
}
