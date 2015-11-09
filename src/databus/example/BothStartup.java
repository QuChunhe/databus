package databus.example;

import java.util.concurrent.TimeUnit;

import databus.core.Listener;
import databus.network.Client;
import databus.network.Publisher;
import databus.network.Server;
import databus.network.Subscriber;
import databus.util.InternetAddress;

public class BothStartup {

    public static void main(String[] args) throws InterruptedException {
        
        Configuration config = Configuration.instance();
        InternetAddress localAddress = config.loadListeningAddress();
        Server server = new Server(localAddress);
        Client client = new Client(localAddress);
        
        Publisher publisher = new Publisher(client);
        Subscriber subscriber = new Subscriber(client);
        subscriber.setMaxSubscribingPeroid(10, TimeUnit.SECONDS);
        server.setPublisher(publisher).setSubscriber(subscriber);
        
        Thread serverThread = server.start();       
        Thread clientThread = client.start();
        
        Listener listener = config.loadListeners(publisher);
        config.loadReceivers(subscriber);

        Thread.sleep(500);
        
        subscriber.subscribe();
        
        Thread.sleep(500);
       
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
