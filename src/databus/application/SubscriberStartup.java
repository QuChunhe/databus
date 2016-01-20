package databus.application;


import databus.network.Server;
import databus.network.Subscriber;
import databus.util.InternetAddress;

public class SubscriberStartup {

    public static void main(String[] args) {        
        Configurations config = new Configurations("conf/subscriber.xml");
        InternetAddress localAddress = config.loadServerAddress();
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
        System.exit(0);
    }
}
