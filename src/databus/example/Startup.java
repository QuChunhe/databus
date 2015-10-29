package databus.example;

import databus.core.Listener;
import databus.listener.MysqlListener;
import databus.network.PublishingServer;
import databus.network.SubscribingSever;

public class Startup {

    public static void main(String[] args) throws InterruptedException {
        PublishingServer publisher = new PublishingServer();
        SubscribingSever subscriber = new SubscribingSever(publisher);
        Listener listener = new MysqlListener(publisher);
        Thread publisherThread = publisher.start();
        Thread subscriberThread = subscriber.start();
        
        Thread.sleep(500);
        
        subscriber.subscribe();
        
        Thread.sleep(500);
       
        listener.start();
        try {
            publisherThread.join();
            subscriberThread.join();
        } finally {
            publisher.stop();
            subscriber.stop();
        }
    }
}
