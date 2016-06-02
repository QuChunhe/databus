package databus.network.kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import databus.network.AbstractSubscriber;
import databus.network.JsonEventParser;


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
        String kafkaPropertieFile = properties.getProperty("kafka.consumerConfig");
        if (null == kafkaPropertieFile) {
            log.error("Must configure 'consumerConfig' for 'kafka'");
            System.exit(1);
        }
        kafkaProperties = new Properties();
        try {
            kafkaProperties.load(Files.newBufferedReader(Paths.get(kafkaPropertieFile)));
            log.info(kafkaProperties.toString());
        } catch (IOException e) {
            log.error("Can't load properties file "+kafkaPropertieFile, e);
            System.exit(1);
        }

        if (null == kafkaProperties.getProperty("group.id")) {
            kafkaProperties.setProperty("group.id", 
                                        "default-" + Math.round(Math.random()*1000000));
        }

        kafkaProperties.setProperty("key.deserializer", 
                                    "org.apache.kafka.common.serialization.LongDeserializer");
        kafkaProperties.setProperty("value.deserializer", 
                                    "org.apache.kafka.common.serialization.StringDeserializer");

        if (null == executor) {
            executor = KafkaHelper.loadExecutor(properties, 0);        
        }
        
        String  fromBeginningValue = properties.getProperty("kafka.fromBeginning");
        if (null != fromBeginningValue) {
            doesSeekFromBeginning = Boolean.parseBoolean(fromBeginningValue);
        }
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
                            log.info(p.partition() + " " + r.topic()+ " : " + event.toString());
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
        kafkaProperties.put("bootstrap.servers", servers.toString());

        consumer = new KafkaConsumer<Long, String>(kafkaProperties);
        List<String> topicList = new ArrayList<String>(receiversMap.size());
        topicList.addAll(receiversMap.keySet());
        consumer.subscribe(topicList, 
                           new AutomaticRebalanceListener(consumer, doesSeekFromBeginning));
        log.info(topicList.toString());
    }
    
    private static Log log = LogFactory.getLog(KafkaSubscriber.class);
    private static JsonEventParser eventParser = new JsonEventParser();
    
    private KafkaConsumer<Long, String> consumer;
    private Executor executor = null;
    private Properties kafkaProperties; 
    boolean doesSeekFromBeginning = true;

}
