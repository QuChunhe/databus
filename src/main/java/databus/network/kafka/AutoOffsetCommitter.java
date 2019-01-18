package databus.network.kafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Qu Chunhe on 2019-01-07.
 */
public class AutoOffsetCommitter<K, V> extends OffsetCommitter<K, V> {

    @Override
    public void initialize(KafkaConsumer<K, V> consumer) {
        super.initialize(consumer);

        if (null == startOffsetsMap) {
            return;
        }
        consumer.poll(0);
        for(TopicPartition partition : consumer.assignment()) {
            String t = partition.topic();
            Map<Integer, Long> offset = startOffsetsMap.get(t);
            if (null == offset){
                continue;
            }
            Long o = offset.get(partition.partition());
            if (null == o) {
                continue;
            }
            consumer.seek(partition, o.longValue());
        }
    }

    @Override
    public void afterPolling() {
        consumer.commitAsync(OFFSET_COMMIT_CALLBACK);
    }

    @Override
    boolean beforeProcessing(String topic, int partition, long offset) {
        return !hasReceivedBefore(topic, partition, offset);
    }

    @Override
    public void afterProcessing(String topic, int partition, long offset) {
    }

    @Override
    public void beforePolling() {
    }

    @Override
    public void close() throws Exception {
        consumer.commitSync();
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

    public void setStartOffsetsMap(Map<String, Map<Integer, Long>> startOffsetsMap) {
        this.startOffsetsMap = startOffsetsMap;
    }

    private boolean hasReceivedBefore(String topic, int partition, long offset) {
        Map<Integer, Long> partitionMap = previousOffsetMap.get(topic);
        if (null == partitionMap) {
            partitionMap = new HashMap<>();
            previousOffsetMap.put(topic, partitionMap);
        }
        Long previousOffset = partitionMap.get(partition);
        if ((null==previousOffset) || (offset>previousOffset.longValue())) {
            partitionMap.put(partition, offset);
            return false;
        }

        return true;
    }

    private final OffsetCommitCallback OFFSET_COMMIT_CALLBACK =
            new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                       Exception exception) {
                    if (null != exception) {
                        log.error("Can not commit offsets : "+offsets.toString(), exception);
                    }
                }
            };

    private final static Log log = LogFactory.getLog(AutoOffsetCommitter.class);

    private final Map<String, Map<Integer, Long>> previousOffsetMap = new HashMap<>();
    private Map<String, Map<Integer, Long>> startOffsetsMap;
}
