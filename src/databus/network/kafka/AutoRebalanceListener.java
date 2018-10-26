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
        int waitingTime = 0;
        boolean doesCommitOffset = false;
        while (!doesCommitOffset && (waitingTime<=30)) {
            if (waitingTime > 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.error("Has waked up", e);
                }
            }
            try {
                consumer.commitSync();
                doesCommitOffset = true;
            } catch (Exception e) {
                log.error("Can not commitSync", e);
            }
            waitingTime++;
        }
        if (!doesCommitOffset) {
            log.fatal("Can not commit offset");
        }
    }

    private final static Log log = LogFactory.getLog(AutoRebalanceListener.class);
    
    private final KafkaConsumer<String, String> consumer;
}
