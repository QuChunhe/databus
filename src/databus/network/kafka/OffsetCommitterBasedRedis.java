package databus.network.kafka;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Created by Qu Chunhe on 2019-01-08.
 */
public class OffsetCommitterBasedRedis<K, V> extends OffsetCommitter<K, V> {

    public OffsetCommitterBasedRedis() {
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
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
        try (Jedis jedis = jedisPool.getResource()) {
            setOffset(jedis, topic, partition, offset);
        } catch(Exception e) {
            log.error("Redis can not set "+topic+"("+partition+", "+offset+")", e);
        }
    }

    @Override
    void beforePolling() {
    }

    @Override
    public void close() throws Exception {
        jedisPool.close();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        HashSet<TopicPartition> topicPartitions = null;
        for(TopicPartition p : partitions) {
            long offset = getOffset(p.topic(), p.partition());
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

    private long getOffset(String topic, int partition) {
        long offset = -1;
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(toKey(topic, partition));
            if (null != value) {
                offset = Long.parseUnsignedLong(value);
            }
        } catch(Exception e) {
            log.error("Redis can not set "+topic+"("+partition+", "+offset+")", e);
        }

        return offset;
    }

    private void setOffset(Jedis jedis,String topic, int partition, long offset) {
        while (true) {
            String replacedValue = jedis.getSet(toKey(topic, partition), Long.toString(offset));
            if ((null==replacedValue) || "".equals(replacedValue)) {
                return;
            }
            long replacedOffset = Long.parseUnsignedLong(replacedValue);
            if (replacedOffset <= offset) {
                return;
            }
            offset = replacedOffset;
        }
    }

    private String toKey(String topic, int partition) {
        return keyPrefix+":"+topic+":"+partition;
    }

    private final static Log log = LogFactory.getLog(OffsetCommitterBasedRedis.class);

    private JedisPool jedisPool;
    private String keyPrefix = "kafka";
}
