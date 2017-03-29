package databus.network.kafka;

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
import databus.network.AbstractSubscriber;
import databus.network.JsonEventParser;
import databus.network.Transporter;
import databus.util.Helper;

public class KafkaSubscriber extends AbstractSubscriber {
    
    public KafkaSubscriber() {
        super();
        serverTopicsMap = new HashMap<>();
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

    public void setConfigFileName(String configFileName) {
        kafkaProperties = Helper.loadProperties(configFileName);
        if (null == kafkaProperties.getProperty("group.id")) {
            kafkaProperties.setProperty("group.id",
                                        "default-" + Math.round(Math.random()*1000000));
        }
        kafkaProperties.setProperty("key.deserializer",
                                    "org.apache.kafka.common.serialization.LongDeserializer");
        kafkaProperties.setProperty("value.deserializer",
                                    "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public void setPollingTimeout(long pollingTimeout) {
        this.pollingTimeout = pollingTimeout;
    }

    public void setWriteNumberPerFlush(int writeNumberPerFlush) {
        positionsCache = new PositionsCache(writeNumberPerFlush);
    }

    @Override
    protected Transporter[] createTransporters() {
        Transporter[] transporters = new PollingTransporter[serverTopicsMap.size()];
        int i = 0;
        for(String server : serverTopicsMap.keySet()) {
            transporters[i++] = new PollingTransporter(server, serverTopicsMap.get(server));
        }
        //help GC
        kafkaProperties = null;
        serverTopicsMap = null;
        return transporters;
    }

    private final static Log log = LogFactory.getLog(KafkaSubscriber.class);
    private final static JsonEventParser eventParser = new JsonEventParser();
    
    private Map<String, Set<String>> serverTopicsMap;
    private long pollingTimeout = 2000;
    private PositionsCache positionsCache = new PositionsCache(1);
    private Properties kafkaProperties;
   
    private class PollingTransporter implements Transporter {

        public PollingTransporter(String server, Set<String> kafkaTopics) {
            properties = new Properties();
            properties.putAll(kafkaProperties);
            String groupId = properties.getProperty("group.id");
            properties.put("client.id", groupId+"-"+server.replaceAll(":", "-"));
            properties.put("bootstrap.servers", server);
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
