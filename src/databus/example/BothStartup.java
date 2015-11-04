package databus.example;

import databus.core.Listener;
import databus.listener.MysqlListener;
import databus.network.Client;
import databus.network.Publisher;
import databus.network.Server;
import databus.network.Subscriber;
import databus.util.Configuration;
import databus.util.InternetAddress;

public class BothStartup {

    public static void main(String[] args) throws InterruptedException {
        
        InternetAddress localAddress = Configuration.instance()
                                                    .loadListeningAddress();
        Server server = new Server(localAddress);
        Client client = new Client(localAddress);
        
        Publisher publisher = new Publisher(client);
        Subscriber subscriber = new Subscriber(client); 
        server.setPublisher(publisher).setSubscriber(subscriber);
        
        Thread serverThread = server.start();       
        Thread clientThread = client.start();
        
        Listener listener = new MysqlListener(publisher);
        
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
        }
    }
}