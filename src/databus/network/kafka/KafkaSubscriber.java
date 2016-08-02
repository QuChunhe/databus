package databus.network.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class KafkaSubscriber extends AbstractKafkaSubscriber {    

    public KafkaSubscriber() {
        this(null, 1, "KafkaSubscriber");
    }

    public KafkaSubscriber(ExecutorService executor, int pollingThreadNumber, String name) {
        super(executor, pollingThreadNumber, name);
    }

    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);
        String saveThresholdValue = properties.getProperty("kafka.writePerFlush");
        if (null != saveThresholdValue) {
            saveThreshold = Integer.parseUnsignedInt(saveThresholdValue);
        }
        if (saveThreshold < 1) {
            saveThreshold = 1;
        }
        cacheCounters = new ConcurrentHashMap<String, AtomicInteger>();
        positionCache = new PositionsCache();
    }    

    /**
     * thread-safe method
     */
    @Override
    protected void cachePosition(String topic, int partition, long position) {
        positionCache.set(topic, partition, position);
        AtomicInteger counter = cacheCounters.get(topic);
        if (null == counter) {
            synchronized(this) {
                if (null == counter) {
                    counter = new AtomicInteger(0);
                    cacheCounters.put(topic, counter);
                }
            }            
        }        
        int currentCount = counter.incrementAndGet();
        do {
           if (counter.compareAndSet(currentCount, 0)) {
                positionCache.save(topic);
                break;
            } 
        } while ((currentCount=counter.get()) >= saveThreshold);
    }
    
    @Override
    protected Worker createWorker() {
        return new KafkaConsumerWorker();
    }
    
    @Override
    protected boolean isLegal(ConsumerRecord<Long, String> record) {
        return record.offset() > positionCache.get(record.topic(), record.partition());
    }

    private Map<String,AtomicInteger> cacheCounters;
    private PositionsCache positionCache;    
    private int saveThreshold = 1; 
    
    
    protected class KafkaConsumerWorker extends AbstractKafkaConsumerWorker {

        public KafkaConsumerWorker() {
            super();
        }

        @Override
        public void initialize() {
            super.initialize();
            KafkaHelper.seekRightPositions(consumer, consumer.assignment());  
        }
    }
}
