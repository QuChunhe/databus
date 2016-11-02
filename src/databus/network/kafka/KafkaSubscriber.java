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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import databus.core.Event;
import databus.core.Receiver;
import databus.core.Runner;
import databus.network.AbstractSubscriber;
import databus.network.JsonEventParser;

public class KafkaSubscriber extends AbstractSubscriber {
    
    public KafkaSubscriber() {
        super();
    }

    @Override
    public void register(String remoteTopic, Receiver receiver) {
        String server = KafkaHelper.splitSocketAddress(remoteTopic); 
        String topic = KafkaHelper.splitTopic(remoteTopic);
        if ((null==server) || (null==topic)) {
            log.error("remoteTopic is illegal : "+remoteTopic);
            System.exit(1);
        }
        String normalizedTopic = topic.replace('/', '-')
                                      .replace('.', '-')
                                      .replace(':', '-')
                                      .replace('_', '-');
        super.register(server+"/"+normalizedTopic, receiver);
        
        Set<String> topicSet = serverTopicsMap.get(server);
        if (null == topicSet) {
            topicSet = new HashSet<>();
            serverTopicsMap.put(server, topicSet);
        }
        topicSet.add(normalizedTopic);
    }

    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);
        
        if (null != properties.getProperty("kafka.pollingTimeout")) {
            pollingTimeout = Long.parseLong(properties.getProperty("kafka.pollingTimeout"));
        }
        
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
        String writePerFlushValue = properties.getProperty("kafka.writePerFlush");
        int writePerFlush = null==writePerFlushValue ? 1 : Integer.parseInt(writePerFlushValue);
        writePerFlush = (writePerFlush<1) ? 1 : writePerFlush;  
        positionsCache = new PositionsCache(writePerFlush);
    }

    @Override
    protected Runner[] createTransporters() {
        Runner[] runners = new Runner[serverTopicsMap.size()];
        int i = 0;
        for(String server : serverTopicsMap.keySet()) {
            runners[i++] = new PollingRunner(kafkaProperties, server,serverTopicsMap.get(server));
        }
        //help GC
        kafkaProperties = null;
        serverTopicsMap = null;
        return runners;
    } 
       
    private static Log log = LogFactory.getLog(KafkaSubscriber.class);
    private static JsonEventParser eventParser = new JsonEventParser(); 
    
    private Properties kafkaProperties;
    private Map<String, Set<String>> serverTopicsMap = new HashMap<>();
    private long pollingTimeout = 2000;
    private PositionsCache positionsCache;  
    
   
    private class PollingRunner implements Runner {        

        public PollingRunner(Properties properties, String server, Set<String> kafkaTopics) {
            this.properties = new Properties();
            this.properties.putAll(properties);
            String groupId = this.properties.getProperty("group.id");
            this.properties.put("client.id", groupId+"-"+server.replaceAll(":", "-"));
            this.properties.put("bootstrap.servers", server);            
            this.kafkaTopics = new ArrayList<>(kafkaTopics);
            this.server = server;
            log.info(server+" "+kafkaTopics.toString());
        }

        @Override
        public void initialize() {
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(kafkaTopics, new AutoRebalanceListener(server, consumer)); 
            KafkaHelper.seekRightPositions(server, consumer, consumer.assignment());
            //help GC
            kafkaTopics = null;
            properties = null;
        }

        @Override
        public void runOnce() throws Exception {
            ConsumerRecords<Long, String> records = consumer.poll(pollingTimeout);
            if ((null!=records) && (!records.isEmpty())) {               
                for (ConsumerRecord<Long, String> r : records) {
                    String topic = r.topic();
                    int partition = r.partition();
                    long offset = r.offset();
                    long key = r.key();
                    String fullTopic = server + "/" + topic;  
                    String logPrefix = key + " " + fullTopic + " (" + partition + ", " + offset + ")";                    
                    if (offset <= positionsCache.get(fullTopic, partition)) {
                        log.warn(logPrefix + " is processed ahead : " + r.value());
                        continue;
                    } else {
                        positionsCache.set(fullTopic, partition, offset);
                    }
                    
                    Event event = eventParser.toEvent(r.value()); 
                    if (null == event) {
                        log.error("message can not be parser as an event " + logPrefix+ " : " +
                                  r.value());
                        continue;
                    } 
                    log.info(logPrefix + " : " + event.toString());
                    event.topic(topic);                    
                    receive(fullTopic, event);                    
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
            while (owner.isAlive()) {
                log.info("Wake up consumer!");
                consumer.wakeup();
                log.info("Waiting consumer to finish!");
                try {
                    owner.join(1000);
                } catch (InterruptedException e) {
                    //do nothing
                }                
            }
        }

        @Override
        public void close() {
            consumer.close();
        }
        
        private Properties properties;
        private List<String> kafkaTopics;
        private KafkaConsumer<Long, String> consumer;
        private String server;
    }
}
