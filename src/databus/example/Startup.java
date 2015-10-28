package databus.example;

import databus.core.Listener;
import databus.listener.MysqlListener;
import databus.network.PublishingServer;
import databus.network.SubscribingSever;

public class Startup {

    public static void main(String[] args) {
        PublishingServer publisher = new PublishingServer();
        SubscribingSever subscriber = new SubscribingSever(publisher);
        Listener listener = new MysqlListener(publisher);
        Thread publisherThread = publisher.start();
        Thread subscriberThread = subscriber.start();
        listener.start();
        try {
            publisherThread.join();
            subscriberThread.join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        System.out.println("*******************************");   
        

    }

}
