package databus.network.kafka;

import java.util.Set;

import databus.util.Backup;

public class PositionsCache {
    
    public PositionsCache() {
        this(null);
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

        return this;
    }
    
    public PositionsCache save(String topic) {
        if ((null==topicSet) || topicSet.contains(topic)) {
            Backup.instance().getRecordCache(topic).save();
        }
        return this;
    }  
    
    public PositionsCache saveAll() {
        if (null == topicSet) {
            return this;
        }
        for(String topic: topicSet) {
            Backup.instance().getRecordCache(topic).save();
        }
        return this;
    }

    private Set<String> topicSet;
    
}
