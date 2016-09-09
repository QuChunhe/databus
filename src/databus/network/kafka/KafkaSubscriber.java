package databus.network.kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
        serverTopicsMap = new HashMap<String, List<String>>();
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
        String topic = KafkaHelper.splitTopic(remoteTopic)
                                  .replace('/', '-')
                                  .replace(':', '-')
                                  .replace('_', '-');
        remoteTopic = remoteTopic.replace('/', '-')
                                 .replace(':', '-')
                                 .replace('_', '-');
        super.register(remoteTopic, receiver); 
        log.info(remoteTopic);
        
        List<String> topicsList = serverTopicsMap.get(server);
        if (null == topicsList) {
            topicsList = new LinkedList<String>();
            serverTopicsMap.put(server, topicsList);
        }
        topicsList.add(topic);
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
            runners[i++] = new PollingRunner(kafkaProperties, server, serverTopicsMap.get(server));
        }
        kafkaProperties = null;
        serverTopicsMap = null;
        return runners;
    }

    private void receive0(String topic, int partition, long position, Event event) {
        receive(topic, event);
        positionsCache.set(topic, partition, position);
    }
    
    private void receive(String server, ConsumerRecord<Long, String> record) {
        Event event = eventParser.toEvent(record.value());
        log.info(record.key() + " " + record.topic() + " (" + record.partition() + "," + 
                 record.offset() + ") : " + event.toString());
        if (record.offset() <= positionsCache.get(record.topic(), record.partition())) {
            log.warn(record.topic() + " (" + record.partition() + "," + record.offset() +
                     ") is illegal");
        } else if (null != executor) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    receive0(server + record.topic(), record.partition(), record.offset(), event);
                    
                }
            });
        } else {
            receive0(server + record.topic(), record.partition(), record.offset(), event);
        }
    }    
       
    private static Log log = LogFactory.getLog(KafkaSubscriber.class);
    private static JsonEventParser eventParser = new JsonEventParser(); 

    private ExecutorService executor = null;
    private Properties kafkaProperties;
    private Map<String, List<String>> serverTopicsMap;
    private long pollingTimeout = 2000;
    private PositionsCache positionsCache;  
    
   
    private class PollingRunner implements Runner {        

        public PollingRunner(Properties properties, String server, List<String> topics) {
            this.properties = new Properties();
            this.properties.putAll(properties);
            String groupId = this.properties.getProperty("group.id");
            this.properties.put("client.id", groupId+"-"+server.replaceAll(":", "-"));
            this.properties.put("bootstrap.servers", server);
            
            this.topics = topics;
            this.server = server.replace('/', '-')
                                .replace(':', '-')
                                .replace('_', '-');
        }

        @Override
        public void initialize() {
            consumer = new KafkaConsumer<Long, String>(properties); 
            consumer.subscribe(topics, new AutoRebalanceListener(consumer)); 
            KafkaHelper.seekRightPositions(consumer, consumer.assignment());
            
            topics = null;
            properties = null;
        }

        @Override
        public void runOnce() throws Exception {
            ConsumerRecords<Long, String> records = consumer.poll(pollingTimeout);
            if ((null!=records) && (!records.isEmpty())) {                   
                for(ConsumerRecord<Long, String> r : records) {
                    receive(server, r);
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
        List<String> topics;
        private KafkaConsumer<Long, String> consumer;
        private String server;
    }
}
