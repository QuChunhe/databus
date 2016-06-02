package databus.network.kafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import databus.util.Backup;

public class AutomaticRebalanceListener implements ConsumerRebalanceListener{
    
    public AutomaticRebalanceListener(KafkaConsumer<Long, String> consumer, 
                                      boolean doesSeekFromBeginning) {;
        this.consumer = consumer;
        this.doesSeekFromBeginning = doesSeekFromBeginning;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        HashMap<String, Map<String, String>> cache = new HashMap<String, Map<String, String>>();
        for(TopicPartition p : partitions) {
            String topic = p.topic();
            Map<String, String> data = cache.get(topic);
            if (null == data) {
                data = Backup.instance().restore(topic);
                if (null != data) {
                    cache.put(topic, data);
                }
            }
            String partitionNum = Integer.toString(p.partition());
            if ((null==data) || (data.get(partitionNum)==null)) {
                if (doesSeekFromBeginning) {
                    consumer.seekToBeginning(p);
                }
            } else {
                long offset = Long.parseLong(data.get(partitionNum));
                consumer.seek(p, offset);
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        HashMap<String, Map<String, String>> cache = new HashMap<String, Map<String, String>>();
        for(TopicPartition p : partitions) {
            String topic = p.topic();
            Map<String, String> data = cache.get(topic);
            if (null == data) {
                data = Backup.instance().restore(topic);
                if (null == data) {
                    data = new HashMap<String, String>();
                }
                cache.put(topic, data);
            }
            data.put(Integer.toString(p.partition()), 
                     Long.toString(consumer.position(p)));
        }
        for(String topic : cache.keySet()) {
            Backup.instance().store(topic, cache.get(topic));
        }
    }
    
    private KafkaConsumer<Long, String> consumer;
    private boolean doesSeekFromBeginning;
}
