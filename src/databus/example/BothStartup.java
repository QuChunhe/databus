package databus.example;

import databus.listener.BatchListener;
import databus.network.Client;
import databus.network.Publisher;
import databus.network.Server;
import databus.network.Subscriber;
import databus.util.InternetAddress;

public class BothStartup {

    public static void main(String[] args) throws InterruptedException {
        
        Configurations config = new Configurations();
        InternetAddress localAddress =config.loadServerAddress();
        Server server = new Server(localAddress);
        Client client = new Client(localAddress);        

        Publisher publisher = new Publisher(client);
        Subscriber subscriber = new Subscriber();
        server.setPublisher(publisher).setSubscriber(subscriber);
        
        Thread serverThread = server.start();       
        Thread clientThread = client.start();
        
        BatchListener listener = config.loadListeners();
        listener.setPublisher(publisher);
        config.loadReceivers(subscriber);
        config.loadSubscribers(publisher);        

        listener.start();
        try {
            serverThread.join();
            clientThread.join();
        } finally {
            client.stop();
            server.stop();
            listener.stop();
        }
        System.exit(0);
    }
}
