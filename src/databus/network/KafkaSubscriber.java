package databus.network;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import databus.core.Event;
import databus.core.Receiver;


public class KafkaSubscriber extends AbstractSubscriber {
    
    public KafkaSubscriber() {
        super(new HashMap<String, Set<Receiver>>());
    }
    
    public KafkaSubscriber(Executor executor) {
        this();
        this.executor = executor;
    }

    @Override
    public void stop() {
        super.stop();
        consumer.close();
    }    

    @Override
    public void register(String topic, Receiver receiver) {
        super.register(topic, receiver);        
    }

    @Override
    public void initialize(Properties properties) {
        String groupId = properties.getProperty("kafka.groupId");
        if (null == groupId) {
            groupId = "default" + Math.round(Math.random()*1000000);
        }
        kafkaConfigs = new HashMap<String, Object>(16);        
        kafkaConfigs.put("group.id", groupId);
        kafkaConfigs.put("enable.auto.commit", true);
        kafkaConfigs.put("auto.commit.interval.ms", 1000);
        kafkaConfigs.put("session.timeout.ms", 30000);
        kafkaConfigs.put("key.deserializer", 
                         "org.apache.kafka.common.serialization.LongDeserializer");
        kafkaConfigs.put("value.deserializer", 
                         "org.apache.kafka.common.serialization.StringDeserializer");     

        if (null == executor) {
            executor = KafkaHelper.loadExecutor(properties, 0);        
        }
        
        String  fromBeginningValue = properties.getProperty("kafka.fromBeginning");
        boolean doesPollFromBeginning = true;
        if (null != fromBeginningValue) {
            doesPollFromBeginning = Boolean.parseBoolean(fromBeginningValue);
        }
        String offsetReset = doesPollFromBeginning ? "earliest" : "latest";
        kafkaConfigs.put("auto.offset.reset", offsetReset);
    }

    @Override
    protected void run0() {
        initialize0();

        while (true) {
            try {
                ConsumerRecords<Long, String> records = consumer.poll(3600);
                if ((null != records) && (!records.isEmpty())) {
                    for(TopicPartition p : records.partitions()) {
                        for(ConsumerRecord<Long, String> r : records.records(p)) {
                            Event event = eventParser.toEvent(r.value());
                            if (null != executor) {
                                executor.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        receive0(r.topic(), event);
                                    }
                                });
                            } else {
                                receive(event);
                            }
                            log.info(p.partition() + " " + r.topic()+ " : " + event.toString());
                        }
                    }
                }
            } catch(Exception e) {
                log.error("Exception Throwsn when polling Kafka", e);
            }
        }
    }    

    private void initialize0() {
        HashSet<String> serverSet = new HashSet<String>();
        Map<String, Set<Receiver>> newMap = new ConcurrentHashMap<String, Set<Receiver>>();
        for(String t : receiversMap.keySet()) {;
            newMap.put(t.replace('/', '-')
                        .replace(':', '-')
                        .replace('_', '-'),
                       receiversMap.get(t));  
            String address = KafkaHelper.splitSocketAddress(t);
            if (null == address) {
                log.error("remoteTopic " + t + " is illegal");
                System.exit(1);
            }
            serverSet.add(address);            
        }
        receiversMap = newMap;
        StringBuilder servers = new StringBuilder(64);
        for(String s : serverSet) {
            if (servers.length() > 0) {
                servers.append(',');
            }
            servers.append(s);
        }
        kafkaConfigs.put("bootstrap.servers", servers.toString());

        consumer = new KafkaConsumer<Long, String>(kafkaConfigs);
        List<String> topicList = new ArrayList<String>(receiversMap.size());
        topicList.addAll(receiversMap.keySet());
        consumer.subscribe(topicList);
        log.info(topicList.toString());
    }
    
    private static Log log = LogFactory.getLog(KafkaSubscriber.class);
    private static JsonEventParser eventParser = new JsonEventParser();
    
    private KafkaConsumer<Long, String> consumer;
    private Executor executor = null;
    private Map<String, Object> kafkaConfigs;    

}
