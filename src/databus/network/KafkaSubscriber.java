package databus.network;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import databus.core.Event;


public class KafkaSubscriber extends AbstractSubscriber {
    
    public KafkaSubscriber() {
        super();
    }

    @Override
    public void stop() {
        super.stop();
        consumer.close();
    }

    @Override
    public void initialize(Properties properties) {
        String servers = properties.getProperty("kafka.servers");
        String groupId = properties.getProperty("kafka.groupId");
        if (null == groupId) {
            groupId = "default";
        }
        Map<String, Object> configs = new HashMap<String, Object>(16);
        configs.put("bootstrap.servers", servers);
        configs.put("group.id", groupId);
        configs.put("enable.auto.commit", true);
        configs.put("auto.commit.interval.ms", 1000);
        configs.put("session.timeout.ms", 30000);
        configs.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        consumer = new KafkaConsumer<Long, String>(configs);        
    }

    @Override
    protected void run0() {
        LinkedList<String> topics = new LinkedList<String>();
        for(String t : receiversMap.keySet()) {
            log.info(t.replace('/', '-'));
            topics.add(t.replace('/', '-'));
        }
        consumer.subscribe(topics);        
        
        while (true) {
            try {
                ConsumerRecords<Long, String> records = consumer.poll(3600);
                if ((null != records) && (!records.isEmpty())) {
                    System.out.println(records.count());
                    for(TopicPartition p : records.partitions()) {
                        for(ConsumerRecord<Long, String> r : records.records(p)) {                         
                            Event event = eventParser.toEvent(r.value());
                            log.info(event.toString());
                            receive(event);
                        }
                    }
                }
            } catch(Exception e) {
                log.error("Exception Throws", e);
            }
        }   
        
    }
    
    private static Log log = LogFactory.getLog(KafkaSubscriber.class);
    private static JsonEventParser eventParser = new JsonEventParser();
    
    private KafkaConsumer<Long, String> consumer;

}
