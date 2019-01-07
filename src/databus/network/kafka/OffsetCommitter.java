package databus.network.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public abstract class OffsetCommitter<K, V> implements ConsumerRebalanceListener, AutoCloseable {

    void initialize(KafkaConsumer<K, V> consumer) {
        this.consumer = consumer;
    }

    abstract void afterPolling();

    abstract boolean beforeProcessing(String topic, int partition, long position);

    abstract void afterProcessing(String topic, int partition, long position);

    abstract void beforePolling();

    protected KafkaConsumer<K, V> consumer;
}
