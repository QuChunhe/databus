package databus.network;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Receiver;

public class KafkaBatchSubscribers implements Subscriber {    

    public KafkaBatchSubscribers() {
        subscribers = new HashMap<String, KafkaSubscriber>();
    }

    @Override
    public void initialize(Properties properties) {
        this.properties = properties;
        executor = KafkaHelper.loadExecutor(properties, 1);
    }

    @Override
    public void join() throws InterruptedException {
        for(KafkaSubscriber s : subscribers.values()) {
            s.join();
        }        
    }

    @Override
    public boolean isRunning() {
        boolean doesHaltAll = true;
        for(KafkaSubscriber s : subscribers.values()) {
            if (s.isRunning()) {
                doesHaltAll = false;
                break;
            }
        }
        return !doesHaltAll;
    }

    @Override
    public void start() {
        for(KafkaSubscriber s : subscribers.values()) {
            s.start();
        }        
    }

    @Override
    public void stop() {
        for(KafkaSubscriber s : subscribers.values()) {
            s.stop();
        }        
    }

    @Override
    public void register(String topic, Receiver receiver) {
        String address = KafkaHelper.splitSocketAddress(topic);
        if (null == address) {
            log.error("remoteTopic " + topic + " is illegal");
            System.exit(1);
        }
        KafkaSubscriber target = subscribers.get(address);
        if (null == target) {
            target = new KafkaSubscriber(executor);
            subscribers.put(address, target);
            target.initialize(properties);
        }
        target.register(topic, receiver);
        
    }
    
    private static Log log = LogFactory.getLog(KafkaBatchSubscribers.class);
    
    private Properties properties;
    private Map<String, KafkaSubscriber> subscribers;
    private Executor executor;

}
