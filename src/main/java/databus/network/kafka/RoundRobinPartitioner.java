package databus.network.kafka;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

/**
 * Created by Qu Chunhe on 2018-06-01.
 */
public class RoundRobinPartitioner extends DefaultPartitioner {
    public RoundRobinPartitioner() {
        super();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return super.partition(topic, null, null, value, valueBytes, cluster);
    }
}
