package databus.network.kafka;

import java.util.Collection;
import java.util.HashSet;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;


public class AutoRebalanceListener implements ConsumerRebalanceListener{
    
    public AutoRebalanceListener(KafkaConsumer<Long, String> consumer, 
                                      boolean doesSeekFromBeginning) {;
        this.consumer = consumer;
        this.doesSeekFromBeginning = doesSeekFromBeginning;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        PositionsCache cache = new PositionsCache(new HashSet<String>());
        for(TopicPartition p : partitions) {
            String topic = p.topic();
            long offset = cache.get(topic, p.partition());
            if (offset >= 0) {
                consumer.seek(p, offset+1);
            } else if (doesSeekFromBeginning) {
                consumer.seekToBeginning(p);;
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        PositionsCache cache = new PositionsCache(new HashSet<String>());
        for(TopicPartition p : partitions) {
            String topic = p.topic();
            long position = consumer.position(p);
            cache.set(topic, p.partition(), position);
        }
        cache.saveAll();
    }
    
    private KafkaConsumer<Long, String> consumer;
    private boolean doesSeekFromBeginning;
}
