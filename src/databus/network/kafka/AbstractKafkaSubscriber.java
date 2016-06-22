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
import databus.network.MultiThreadSubscriber;
import databus.network.JsonEventParser;


public abstract class AbstractKafkaSubscriber extends MultiThreadSubscriber {
    
    public AbstractKafkaSubscriber() {
        this(null, 1, "AbstractKafkaSubscriber");
    }
    
    public AbstractKafkaSubscriber(ExecutorService executor, int pollingThreadNumber, String name) {
        super(pollingThreadNumber, name);
        this.executor = executor;
        consumers = new ConcurrentHashMap<Long, KafkaConsumer<Long, String>>(pollingThreadNumber);
    }    

    @Override
    public void stop() {
        super.stop();
        for(KafkaConsumer<Long, String> c : consumers.values()) {
            c.close();
        }
        if ((null!=executor) && (!executor.isTerminated())) {
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Can't wait the terimination of ExecutorService", e);
            }
        }
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
    }

    @Override
    protected void run0() {
        KafkaConsumer<Long, String> consumer = consumers.get(Thread.currentThread().getId());
        try {
            ConsumerRecords<Long, String> records = consumer.poll(3600);
            if ((null != records) && (!records.isEmpty())) {                   
                for(ConsumerRecord<Long, String> r : records) {
                    Event event = eventParser.toEvent(r.value());
                    log.info(r.key() + " " + r.topic() + " (" + r.partition() + "," + 
                             r.offset() + ") : " + event.toString());
                    if (!isLegal(r)) {
                        log.warn( r.topic() + " (" + r.partition() + "," + r.offset() +
                                  ") is illegal");
                    }else if (null != executor) {
                        executor.execute(new Runnable() {
                            @Override
                            public void run() {
                                receive0(r.topic(), r.partition(), r.offset(), event);
                            }
                        });
                    } else {
                        receive0(r.topic(), r.partition(), r.offset(), event);
                    }
                }
            }
        } catch(Exception e) {
            log.error("Exception Throwsn when polling Kafka", e);
        }
    }
    
    protected abstract void cachePosition(String topic, int partition, long position); 
    
    protected abstract boolean isLegal(ConsumerRecord<Long, String> record);
    
    @Override
    protected void initializeOnce() {
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
    }

    @Override
    protected void initializePerThread() {
        Properties properties = new Properties();
        properties.putAll(kafkaProperties);
        long currentThreadId = Thread.currentThread().getId();
        String clientId = properties.getProperty("group.id")+"-"+currentThreadId;
        properties.setProperty("client.id", clientId);
        KafkaConsumer<Long, String> consumer = new KafkaConsumer<Long, String>(properties);
        List<String> topicList = new ArrayList<String>(receiversMap.size());
        topicList.addAll(receiversMap.keySet());
        consumer.subscribe(topicList, new AutoRebalanceListener(consumer));
        log.info(clientId + " : " + topicList.toString());
        consumers.put(currentThreadId, consumer);
    }
    
    private void receive0(String topic, int partition, long position, Event event) {
        receive(topic, event);
        cachePosition(topic, partition, position);
    }
    
    protected ConcurrentHashMap<Long, KafkaConsumer<Long, String>> consumers;
    
    private static Log log = LogFactory.getLog(AbstractKafkaSubscriber.class);
    private static JsonEventParser eventParser = new JsonEventParser(); 

    private ExecutorService executor = null;
    private Properties kafkaProperties;   
}
