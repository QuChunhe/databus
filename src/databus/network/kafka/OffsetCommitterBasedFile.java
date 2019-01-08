package databus.network.kafka;

import java.util.Collection;
import java.util.HashSet;

import org.apache.kafka.common.TopicPartition;

/**
 * Created by Qu Chunhe on 2019-01-07.
 */
public class OffsetCommitterBasedFile<K, V> extends OffsetCommitter<K, V> {
    public OffsetCommitterBasedFile(String file) {
        this(file, -1);
    }

    public OffsetCommitterBasedFile(String file,int writePerFlush) {
        offsetCache = new OffsetCache(file, writePerFlush);
    }

    @Override
    void afterPolling() {
    }

    @Override
    boolean beforeProcessing(String topic, int partition, long offset) {
        return true;
    }

    @Override
    void afterProcessing(String topic, int partition, long offset) {
        offsetCache.set(topic, partition, offset);
    }

    @Override
    void beforePolling() {
        offsetCache.save();
    }

    @Override
    public void close() throws Exception {
        offsetCache.save();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        HashSet<TopicPartition> topicPartitions = null;
        for(TopicPartition p : partitions) {
            long offset = offsetCache.get(p.topic(), p.partition());
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

    private final OffsetCache offsetCache;
}
