package databus.network;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import databus.core.Event;
import databus.core.Subscriber;

public class BatchSubscriber implements Subscriber{

    public BatchSubscriber() {
        subscribers = new CopyOnWriteArraySet<Subscriber>();
    }
    
    public BatchSubscriber(Collection<Subscriber> subscribers){
        this.subscribers = new CopyOnWriteArraySet<Subscriber>(subscribers);
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
    
    public boolean add(Collection<Subscriber> subscribers) {
        return this.subscribers.addAll(subscribers);
    }
    
    public boolean remove(Subscriber subscrber) {
        return subscribers.remove(subscrber);
    }
    
    public int size() {
        return subscribers.size();
    }
    
    private Set<Subscriber> subscribers;
}
