package databus.network.kafka;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;


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
        String saveThresholdValue = properties.getProperty("kafka.recordSaveThreshold");
        if (null != saveThresholdValue) {
            saveThreshold = Integer.parseUnsignedInt(saveThresholdValue);
        }
        if (saveThreshold < 1) {
            saveThreshold = 1;
        }
        cacheCounters = new ConcurrentHashMap<String, AtomicInteger>();
        positionCache = new PositionsCache();
    }    

    @Override
    protected void initializePerThread() {
        super.initializePerThread();
        KafkaConsumer<Long, String> consumer = consumers.get(Thread.currentThread().getId());
        HashSet<TopicPartition> partitionsFromBeginning = new HashSet<TopicPartition>();
        for(TopicPartition partition : consumer.assignment()) {
            long position = positionCache.get(partition.topic(), partition.partition());
            if (position >= 0) {
                consumer.seek(partition, position+1);
            } else if (doesSeekFromBeginning) {
                partitionsFromBeginning.add(partition);                
            }
        }
        if (partitionsFromBeginning.size() > 0) {
            consumer.seekToBeginning(partitionsFromBeginning);;
        }
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
           if (counter.compareAndSet(currentCount, currentCount-saveThreshold)) {
                positionCache.save(topic);
                break;
            } 
        } while ((currentCount=counter.get()) >= saveThreshold);
    }
    
    @Override
    protected boolean isLegal(ConsumerRecord<Long, String> record) {
        return record.offset() > positionCache.get(record.topic(), record.partition());
    }

    private Map<String,AtomicInteger> cacheCounters;
    private PositionsCache positionCache;    
    private int saveThreshold = 1;   
}
