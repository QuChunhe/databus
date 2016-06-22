package databus.network.kafka;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;


public class AutoRebalanceListener implements ConsumerRebalanceListener{
    
    public AutoRebalanceListener(KafkaConsumer<Long, String> consumer) {;
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        KafkaHelper.seekRightPositions(consumer, partitions);       
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        PositionsCache cache = new PositionsCache(new HashSet<String>());
        for(TopicPartition p : partitions) {
            long nextPosition = -1;
            try {
                nextPosition = consumer.position(p);
            } catch(Exception e) {
                log.error("Can't record the position of "+p.topic()+" partition "+p.partition());
            }
            if (nextPosition > 0) {
                cache.set(p.topic(), p.partition(), nextPosition-1);
            }            
        }
        cache.saveAll();
    }
    
    private static Log log = LogFactory.getLog(AutoRebalanceListener.class);
    
    private KafkaConsumer<Long, String> consumer;
}
