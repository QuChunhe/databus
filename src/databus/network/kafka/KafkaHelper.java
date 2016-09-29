package databus.network.kafka;

import java.util.Collection;
import java.util.HashSet;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import databus.util.Helper;

public class KafkaHelper {
    
    public static String splitSocketAddress(String remoteTopic) {
        int index = remoteTopic.indexOf('/');
        if (index < 1) {
            return null;
        }
        return Helper.normalizeSocketAddress(remoteTopic.substring(0, index));
    }
    
    public static String splitTopic(String remoteTopic) {
        int index = remoteTopic.indexOf('/');
        if (index < 1) {
            return null;
        }
        if (index == remoteTopic.length()) {
            return null;
        }
        return remoteTopic.substring(index+1);
    }
    
    public static void seekRightPositions(String server, KafkaConsumer<Long, String> consumer, 
                                          Collection<TopicPartition> partitions) {
        PositionsCache cache = new PositionsCache();
        HashSet<TopicPartition> topicPartitions = null;
        for(TopicPartition p : partitions) {
            long offset = cache.get(server+"/"+p.topic(), p.partition());
            if (offset < 0) {
                if (null == topicPartitions) {
                    topicPartitions = new HashSet<TopicPartition>();
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
