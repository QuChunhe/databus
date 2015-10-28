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
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        subscriber.subscribe();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        listener.start();
        try {
            publisherThread.join();
            subscriberThread.join();
        } catch (InterruptedException e) {
            publisher.stop();
            subscriber.stop();
            e.printStackTrace();
        }
        
        System.out.println("*******************************");   
        

    }

}
