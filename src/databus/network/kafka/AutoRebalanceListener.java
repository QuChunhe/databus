package databus.network.kafka;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class AutoRebalanceListener implements ConsumerRebalanceListener{
    
    public AutoRebalanceListener(String server, KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
        this.server = server;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        KafkaHelper.seekRightPositions(server, consumer, partitions);       
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        PositionsCache cache = new PositionsCache(new HashSet<>());
        for(TopicPartition p : partitions) {
            long nextPosition = -1;
            try {
                nextPosition = consumer.position(p);
            } catch(Exception e) {
                log.error("Can't record the position of "+p.topic()+" partition "+p.partition());
            }
            if (nextPosition > 0) {
                cache.set(server+"/"+p.topic(), p.partition(), nextPosition-1);
            }            
        }
        cache.saveAll();
    }
    
    private final static Log log = LogFactory.getLog(AutoRebalanceListener.class);
    
    private KafkaConsumer<String, String> consumer;
    private String server;
}
