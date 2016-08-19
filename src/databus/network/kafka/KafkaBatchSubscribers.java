package databus.network.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Receiver;
import databus.core.Subscriber;

public class KafkaBatchSubscribers implements Subscriber {    

    public KafkaBatchSubscribers() {
        subscribers = new HashMap<String, KafkaSubscriber>();
        pollingThreadNumberMap = new HashMap<String, Integer>();
    }

    @Override
    public void initialize(Properties properties) {
        this.properties = properties;
        executor = KafkaHelper.loadExecutor(properties, 1);
        String rawHostValue = properties.getProperty("kafka.pollingThread.host");
        String rawNumberValue = properties.getProperty("kafka.pollingThread.number");
        if ((null==rawHostValue) || (null==rawNumberValue)) {
            return;
        }
        String[] hosts = rawHostValue.split(",");        
        String[] numbers = rawNumberValue.split(",");
        if (hosts.length != numbers.length) {
            log.error("kafka.pollingThread.host and kafka.pollingThread.number " +
                      "must be matched");
            return;
        }
        for(int i=0; i<hosts.length; i++) {
            pollingThreadNumberMap.put(hosts[i].trim(), new Integer(numbers[i]));
        }
    }

    @Override
    public void join() throws InterruptedException {
        for(KafkaSubscriber s : subscribers.values()) {
            s.join();
        }        
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
            Integer number = pollingThreadNumberMap.get(address);
            if (null == number) {
                number = 1;
            }
            target = new KafkaSubscriber(executor, number);
            subscribers.put(address, target);
            target.initialize(properties);
        }
        target.register(topic.trim(), receiver);
        
    }
    
    private static Log log = LogFactory.getLog(KafkaBatchSubscribers.class);
    
    private Properties properties;
    private Map<String, KafkaSubscriber> subscribers;
    private ExecutorService executor;
    private Map<String, Integer> pollingThreadNumberMap;

}
