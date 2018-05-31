package databus.network.kafka;

import java.util.Collection;
import java.util.HashSet;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class KafkaHelper {

    public static void seekRightPositions(KafkaConsumer<String, String> consumer,
                                          Collection<TopicPartition> partitions) {
        PositionsCache cache = new PositionsCache();
        HashSet<TopicPartition> topicPartitions = null;
        for(TopicPartition p : partitions) {
            long offset = cache.get(p.topic(), p.partition());
            if (offset < 0) {
                if (null == topicPartitions) {
                    topicPartitions = new HashSet<>();
                }
                topicPartitions.add(p);
            } else {
                //if the position is out of partition range, 
                //the offset depends on the topic and the value of auto.offset.reset.
                consumer.seek(p, offset+1); 
            }             
        }
        if (null != topicPartitions) {
            consumer.seekToEnd(topicPartitions);
        }
    }  
}
