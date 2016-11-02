package databus.network.kafka;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import databus.util.Backup;

public class PositionsCache {
    
    public PositionsCache() {
        this(null);
    }
    
    public PositionsCache(int writePerFlush) {
        this(null);
        if (writePerFlush < 1) {
            throw new IllegalArgumentException("writePerFlush must be great than 0!");
        }
        this.writePerFlush = writePerFlush;
        cacheCounters = new ConcurrentHashMap<>();
    }
    
    public PositionsCache(Set<String> topicSet) {
        this.topicSet = topicSet;
    }
    
    public long get(String topic, int partition) {
        String value = Backup.instance()
                             .getRecordCache(topic)
                             .get(Integer.toString(partition));
        if (null == value) {
            return -1;
        }
        return Long.parseLong(value);
    }
    
    public PositionsCache set(String topic, int partition, long position) {
        if (null != topicSet) {
            topicSet.add(topic);
        }        
        String key = Integer.toString(partition);
        String value = Long.toString(position);
        while (get(topic, partition) < position) {
            Backup.instance().getRecordCache(topic).cache(key, value);
        }
        if (null == cacheCounters) {
            return this;
        }
        
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
                save(topic);
                break;
            } 
        } while ((currentCount=counter.get()) >= writePerFlush);

        return this;
    }
    
    public PositionsCache save(String topic) {
        if ((null==topicSet) || topicSet.contains(topic)) {
            Backup.instance().getRecordCache(topic).save();
        }
        return this;
    }  
    
    public PositionsCache saveAll() {
        if ((null==topicSet) || (topicSet.size()==0)) {
            return this;
        }
        for(String topic: topicSet) {
            Backup.instance().getRecordCache(topic).save();
        }
        return this;
    }

    private Set<String> topicSet;
    
    private Map<String,AtomicInteger> cacheCounters = null;
    private int writePerFlush = 1;
}
