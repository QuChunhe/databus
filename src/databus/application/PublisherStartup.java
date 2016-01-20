package databus.application;

import databus.listener.BatchListener;
import databus.network.Client;
import databus.network.Publisher;

public class PublisherStartup {

    public static void main(String[] args) {
        Configurations config = new Configurations("conf/publisher.xml");  
        Client client = new Client();        
        Publisher publisher = new Publisher(client);
        config.loadSubscribers(publisher);       
        Thread clientThread = client.start();        
        BatchListener listener = config.loadListeners();
        listener.setPublisher(publisher);
        listener.start();
        try {
            clientThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();

            listener.stop();
        }
        System.exit(0);
    }

}
