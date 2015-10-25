package databus.network;

import java.util.Map;
import java.util.Set;

import databus.core.Subscriber;

public class SubscribingSever {    
    
    public SubscribingSever(Client client) {
        this.client = client;
    }

    private Client client;
    private Map<String,Set<Subscriber>> subscribers;

}
