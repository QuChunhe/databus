package databus.network.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.*;

import databus.core.Event;
import databus.network.EventParser;
import databus.network.AbstractSubscriber;
import databus.network.JsonEventParser;
import databus.network.Transporter;
import databus.util.Helper;

public class KafkaSubscriber extends AbstractSubscriber {
    
    public KafkaSubscriber() {
        super();
    }

    public void setConfigFile(String configFile) {
        Properties properties = Helper.loadProperties(configFile);

        if (null == properties.getProperty("group.id")) {
            properties.setProperty("group.id",
                                   "default-" + Math.round(Math.random()*1000000));
        }
        properties.setProperty("key.deserializer",
                               "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                               "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);
    }

    public void setEventParser(EventParser eventParser) {
        this.eventParser = eventParser;
    }

    public void setPollingTimeout(long pollingTimeout) {
        this.pollingTimeout = pollingTimeout;
    }

    public void setStartOffsetsMap(Map<String, Map<Integer, Long>> startOffsetsMap) {
        this.startOffsetsMap = startOffsetsMap;
    }

    @Override
    protected Transporter createTransporter() {
        return new PollingTransporter();
    }

    private final static Log log = LogFactory.getLog(KafkaSubscriber.class);

    private KafkaConsumer<String, String> consumer;
    private EventParser eventParser = new JsonEventParser();
    private long pollingTimeout = 2000;
    private Map<String, Map<Integer, Long>> startOffsetsMap;


    private class PollingTransporter implements Transporter {

        public PollingTransporter() {
        }

        @Override
        public void initialize() {
            consumer.subscribe(receiversMap.keySet(), new AutoRebalanceListener(consumer));
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
        public void runOnce() throws Exception {
            ConsumerRecords<String, String> records = consumer.poll(pollingTimeout);
            if ((null!=records) && (!records.isEmpty())) {
                consumer.commitAsync(OFFSET_COMMIT_CALLBACK);
                //ensure to process every record
                for (ConsumerRecord<String, String> r : records) {
                    String topic = r.topic();
                    int partition = r.partition();
                    long offset = r.offset();
                    String key = r.key();
                    // for bad network
                    if (hasReceivedBefore(topic, partition, offset)) {
                        log.error("Has Received before : " + topic + "   " +
                                  key + " (" + partition + ", " + offset + ")" );
                        continue;
                    }

                    Event event = eventParser.toEvent(topic, key, r.value());
                    if (null == event) {
                        log.error("value can not be parser as an event : " +
                                  r.value());
                        continue;
                    } 
                    log.info(topic+ "   " +key + " (" + partition + ", " + offset + ")" + " : " +
                             event.toString());
                    receive(topic, event);
                }
            }            
        }

        @Override
        public void processFinally() {
        }

        @Override
        public void processException(Exception e) {
            log.warn("Catch Exception"+e.getClass().getName(), e);            
        }

        @Override
        public void stop(Thread owner) {
            log.info("Wake up consumer!");
            consumer.wakeup();
        }

        @Override
        public void close() {
            try {
                consumer.commitSync();
            }catch (Exception e) {
                log.error("Can not commitSync", e);
            } finally {
                consumer.close();
            }
        }

        private boolean hasReceivedBefore(String topic, int partition, long position) {
            Map<Integer, Long> partitionMap = previousPositionMap.get(topic);
            if (null == partitionMap) {
                partitionMap = new HashMap<>();
                previousPositionMap.put(topic, partitionMap);
            }
            Long previousPosition = partitionMap.get(partition);
            if ((null==previousPosition) || (position>previousPosition.longValue())) {
                partitionMap.put(partition, position);
                return false;
            }

            return true;
        }


        private final Map<String, Map<Integer, Long>> previousPositionMap = new HashMap<>();

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
    }

}