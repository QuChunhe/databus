package databus.example;

import databus.listener.BatchListener;
import databus.network.Client;
import databus.network.Publisher;
import databus.network.Server;
import databus.util.InternetAddress;

public class PublisherStartup {

    public static void main(String[] args) {
        Configurations config = new Configurations("conf/publisher.xml");
        InternetAddress localAddress = config.loadServerAddress();
        Server server = new Server(localAddress);
        Client client = new Client(localAddress);
        
        Publisher publisher = new Publisher(client);
        config.loadSubscribers(publisher);
        
        server.setPublisher(publisher);
        
        Thread serverThread = server.start();       
        Thread clientThread = client.start();
        
        BatchListener listener = config.loadListeners();
        listener.setPublisher(publisher);
        listener.start();
        try {
            serverThread.join();
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
            server.stop();
            listener.stop();
        }
        System.exit(0);
    }

}
