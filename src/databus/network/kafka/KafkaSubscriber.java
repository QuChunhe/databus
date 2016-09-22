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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
    public void stop() {
        super.stop();
        
        log.info("Waiting ExecutorService termination!");
        if ((null!=executor) && (!executor.isTerminated())) {
            try {
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Can't wait the terimination of ExecutorService", e);
            }            
        }
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
            topicSet = new HashSet<String>();
            serverTopicsMap.put(server, topicSet);
        }
        topicSet.add(normalizedTopic);
    }

    @Override
    public void initialize(Properties properties) {
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
        executor = KafkaHelper.loadExecutor(properties, 0);
        
        String writePerFlushValue = properties.getProperty("kafka.writePerFlush");
        int writePerFlush = null==writePerFlushValue ? 1 : Integer.parseInt(writePerFlushValue);
        writePerFlush = (writePerFlush<1) ? 1 : writePerFlush;
  
        positionsCache = new PositionsCache(writePerFlush);
    }

    @Override
    protected Runner[] createBackgroundRunners() {
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
    
    private void receive0(String fullTopic, long key, int partition, long offset, Event event) {
        String logPrefix = key + " " + fullTopic + " (" + partition + ", " + offset + ")"; 
        if (offset <= positionsCache.get(fullTopic, partition)) {
            log.error(logPrefix + " is illegal : " + event.toString());
            return;
        }
        log.info(logPrefix + " : " + event.toString());
        receive(fullTopic, event);
        positionsCache.set(fullTopic, partition, offset);
    }
       
    private static Log log = LogFactory.getLog(KafkaSubscriber.class);
    private static JsonEventParser eventParser = new JsonEventParser(); 
    
    private ExecutorService executor = null;
    private Properties kafkaProperties;
    private Map<String, Set<String>> serverTopicsMap = new HashMap<String, Set<String>>();
    private long pollingTimeout = 2000;
    private PositionsCache positionsCache;  
    
   
    private class PollingRunner implements Runner {        

        public PollingRunner(Properties properties, String server, Set<String> kafkaTopics) {
            this.properties = new Properties();
            this.properties.putAll(properties);
            String groupId = this.properties.getProperty("group.id");
            this.properties.put("client.id", groupId+"-"+server.replaceAll(":", "-"));
            this.properties.put("bootstrap.servers", server);            
            this.kafkaTopics = new ArrayList<String>(kafkaTopics);
            this.server = server;
        }

        @Override
        public void initialize() {
            consumer = new KafkaConsumer<Long, String>(properties); 
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
                for(ConsumerRecord<Long, String> r : records) {
                    String fullTopic = server+"/"+r.topic();
                    Event event = eventParser.toEvent(r.value()); 
                    if (null == event) {
                        log.error("message can not be parser as an event " + r.key() + " " +
                                  fullTopic + " (" + r.partition() + ", " + r.offset() + ") : " +
                                  r.value());
                        continue;
                    } 
                    event.topic(r.topic());
                    if (null != executor) {
                        executor.execute(new Runnable() {
                            @Override
                            public void run() {
                                receive0(fullTopic, r.key(), r.partition(), r.offset(), event);                                
                            }
                        });
                    } else {
                        receive0(fullTopic, r.key(), r.partition(), r.offset(), event);
                    }
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
                }                
            }
        }

        @Override
        public void close() {
            consumer.close();   
            try {
                KafkaSubscriber.this.close();
            } catch (IOException e) {
                log.error("Can't close", e);
            }
        }
        
        private Properties properties;
        private List<String> kafkaTopics;
        private KafkaConsumer<Long, String> consumer;
        private String server;
    }
}
