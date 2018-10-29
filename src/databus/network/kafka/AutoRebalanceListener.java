package databus.network.kafka;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class AutoRebalanceListener implements ConsumerRebalanceListener{
    
    public AutoRebalanceListener(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        try {
            consumer.commitSync();
        } catch (Exception e) {
            log.error("Can not commitSync", e);
        }
    }

    private final static Log log = LogFactory.getLog(AutoRebalanceListener.class);
    
    private final KafkaConsumer<String, String> consumer;
}
