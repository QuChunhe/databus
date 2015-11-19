package databus.example;

import databus.network.Client;

import databus.network.Server;
import databus.network.Subscriber;
import databus.util.InternetAddress;

public class SubscriberStartup {

    public static void main(String[] args) {        
        Configuration config = Configuration.instance();
        config.SERVER_CONFIGURATION_NAME = "conf/subscriber.properties";
        InternetAddress localAddress = config.loadListeningAddress();
        Server server = new Server(localAddress);
        Client client = new Client(localAddress);

        Subscriber subscriber = new Subscriber(client);
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
