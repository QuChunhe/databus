package databus.application;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.listener.BatchListener;
import databus.network.Client;
import databus.network.Publisher;

public class PublisherStartup {

    public static void main(String[] args) {
        log.info("******************************************************************************");
        log.info("PublisherStartup will begin!");
        
        String configFileName = "conf/publisher.xml";
        if (args.length > 0) {
            configFileName = args[0];
        } 
        Configurations config = new Configurations(configFileName);  
        Client client = new Client(config.clientThreadPoolSize());        
        Publisher publisher = new Publisher(client);
        config.loadSubscribers(publisher);       
        Thread clientThread = client.start();        
        BatchListener listener = config.loadListeners();
        listener.setPublisher(publisher);
        listener.start();
        try {
            clientThread.join();
        } catch (InterruptedException e) {
            log.error("Client Thread is interrupted!", e);
        } finally {
            client.stop();
            listener.stop();
        }
        log.info("PublisherStartup has finished!");
        System.exit(0);
    }
    
    private static Log log = LogFactory.getLog(PublisherStartup.class);
}
