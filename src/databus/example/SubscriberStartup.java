package databus.example;

import databus.network.Client;

import databus.network.Server;
import databus.network.Subscriber;
import databus.util.InternetAddress;

public class SubscriberStartup {

    public static void main(String[] args) {        
        Configurations config = new Configurations("conf/subscriber.xml");
        InternetAddress localAddress = config.loadServerAddress();
        Server server = new Server(localAddress);
        Client client = new Client(localAddress);

        Subscriber subscriber = new Subscriber();
        server.setSubscriber(subscriber);
        
        Thread serverThread = server.start();       
        Thread clientThread = client.start();

        config.loadReceivers(subscriber);

        try {
            serverThread.join();
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
            server.stop();
        }
        System.exit(0);
    }
}
