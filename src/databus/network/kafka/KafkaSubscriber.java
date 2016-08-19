package databus.network.kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
        this(null, 1);
    }
    
    public KafkaSubscriber(ExecutorService executor, int pollingThreadNumber) {
        super(pollingThreadNumber);
        this.executor = executor;
    }
    
    @Override
    public void start() {
        initialize0();
        super.start();
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
        if (null == executor) {
            executor = KafkaHelper.loadExecutor(properties, 0);        
        }
        
        String writePerFlushValue = properties.getProperty("kafka.writePerFlush");
        int writePerFlush = null==writePerFlushValue ? 1 : Integer.parseInt(writePerFlushValue);
        writePerFlush = (writePerFlush<1) ? 1 : writePerFlush;
  
        positionsCache = new PositionsCache(writePerFlush);
    }

    @Override
    protected Runner createBackgroundRunner() {
        return new PollingRunner();
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
        StringBuilder serversBuilder = new StringBuilder(64);
        for(String s : serverSet) {
            if (serversBuilder.length() > 0) {
                serversBuilder.append(',');
            }
            serversBuilder.append(s);
        }
        kafkaProperties.put("bootstrap.servers", serversBuilder.toString()); 
        
        topics = new ArrayList<String>(receiversMap.size());
        topics.addAll(receiversMap.keySet());
    }

    private void receive0(String topic, int partition, long position, Event event) {
        receive(topic, event);
        positionsCache.set(topic, partition, position);
    }
    
    private void receive(ConsumerRecord<Long, String> record) {
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
                    receive0(record.topic(), record.partition(), record.offset(), event);
                    
                }
            });
        } else {
            receive0(record.topic(), record.partition(), record.offset(), event);
        }
    }    
       
    private static Log log = LogFactory.getLog(KafkaSubscriber.class);
    private static JsonEventParser eventParser = new JsonEventParser(); 

    private ExecutorService executor = null;
    private Properties kafkaProperties;
    private List<String> topics;
    private long pollingTimeout = 2000;
    private PositionsCache positionsCache;  
    
   
    private class PollingRunner implements Runner {        

        public PollingRunner() {
            properties = new Properties();
        }

        @Override
        public void initialize() {
            properties.putAll(kafkaProperties);
            Long threadId = Thread.currentThread().getId();
            String clientId = properties.getProperty("group.id") + "-" + threadId;
            properties.setProperty("client.id", clientId);
            consumer = new KafkaConsumer<Long, String>(properties); 
            consumer.subscribe(topics, new AutoRebalanceListener(consumer)); 
            log.info(clientId + " : " + topics.toString());
            KafkaHelper.seekRightPositions(consumer, consumer.assignment());
        }

        @Override
        public void runOnce() throws Exception {
            ConsumerRecords<Long, String> records = consumer.poll(pollingTimeout);
            if ((null!=records) && (!records.isEmpty())) {                   
                for(ConsumerRecord<Long, String> r : records) {
                    receive(r);
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
        private KafkaConsumer<Long, String> consumer;
    }
}
