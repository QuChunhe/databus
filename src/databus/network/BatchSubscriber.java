package databus.network;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import databus.core.Event;
import databus.core.Subscriber;

public class BatchSubscriber implements Subscriber{

    public BatchSubscriber(String topic) {
        this.topic = topic;
        subscribers = new CopyOnWriteArraySet<Subscriber>();
    }

    @Override
    public boolean receive(Event event) {
        boolean doSuccessed = true;
        for(Subscriber s : subscribers) {
            doSuccessed = doSuccessed && s.receive(event);
        }
        return doSuccessed;
    }
    
    public boolean add(Subscriber subscriber) {
        return subscribers.add(subscriber);
    }
    
    public boolean remove(Subscriber subscrber) {
        return subscribers.remove(subscrber);
    }
    
    /**
     * 
     * @return aaa.bbb.ccc.ddd:port/MYSQL/serverId
     *         /databaseName/tableName
     */
    public String topic() {
        return topic;
    }
    
    private Set<Subscriber> subscribers;
    private String topic;
}
