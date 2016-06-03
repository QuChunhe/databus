package databus.network.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;


public class KafkaSubscriber extends AbstractKafkaSubscriber {    

    public KafkaSubscriber() {
        super();
    }

    public KafkaSubscriber(ExecutorService executor) {
        super(executor);
    }

    /**
     * thread-safe method
     */
    @Override
    protected void cachePosition(String topic, int partition, long position) {
        System.out.println(topic+"  ("+partition+", "+position+")");
        positionCache.set(topic, partition, position);
        AtomicInteger counter = positionCounters.get(topic);
        int currentCount = 1;
        if (null == counter) {
            counter = new AtomicInteger(1);
            positionCounters.put(topic, counter);
        } else {
            currentCount = counter.incrementAndGet();
        }
        if (currentCount < saveThreshold) {
            return;
        }
        if (counter.compareAndSet(currentCount, currentCount-saveThreshold)) {
            positionCache.save(topic);
        }
        
    }
    
    private Map<String,AtomicInteger> positionCounters = 
                                          new ConcurrentHashMap<String, AtomicInteger>();
    private PositionsCache positionCache = new PositionsCache();
   
}
