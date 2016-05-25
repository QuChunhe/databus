package databus.network;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
        
        if (null == executor) {
            String maxThreadPoolSizeValue = properties.getProperty("kafka.maxThreadPoolSize");
            int maxThreadPoolSize = null==maxThreadPoolSizeValue ? 
                                    MAX_THREAD_POOL_SIZE : 
                                    Integer.parseInt(maxThreadPoolSizeValue);
            executor = new ThreadPoolExecutor(1, maxThreadPoolSize, 100, TimeUnit.SECONDS, 
                                              new LinkedBlockingQueue<Runnable>());
        }
        
        String  fromBeginningValue = properties.getProperty("kafka.fromBeginning");
        if (null != fromBeginningValue) {
            doesPollFromBeginning = Boolean.parseBoolean(fromBeginningValue);
        }
        
        String beginTimeValue = properties.getProperty("kafka.beginTime");
        if (null != beginTimeValue) {
            beginTime = Long.parseUnsignedLong(beginTimeValue);
        }
    }

    @Override
    protected void run0() {
        LinkedList<String> topics = new LinkedList<String>();
        for(String t : receiversMap.keySet()) {;
            topics.add(t.replace('/', '-')
                        .replace(':', '-')
                        .replace('_', '-'));
        } 
        log.info(topics.toString());
        consumer.subscribe(topics);
        Set<TopicPartition> partitions = consumer.assignment();
        if (!doesPollFromBeginning) {            
            consumer.seekToEnd(partitions.toArray(new TopicPartition[partitions.size()]));
        } else {
            consumer.seekToBeginning(partitions.toArray(new TopicPartition[partitions.size()]));
        }
        
        while (true) {
            try {
                ConsumerRecords<Long, String> records = consumer.poll(3600);
                if ((null != records) && (!records.isEmpty())) {
                    for(TopicPartition p : records.partitions()) {
                        for(ConsumerRecord<Long, String> r : records.records(p)) {
                            Event event = eventParser.toEvent(r.value());
                            if (doesPollFromBeginning && (event.time()>=beginTime)) {                                
                                executor.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        receive(event);
                                    }
                                });
                            }
                            log.info(p.partition() + " " + r.topic()+ " : " + event.toString());
                        }
                    }
                }
            } catch(Exception e) {
                log.error("Exception Throws", e);
            }
        }
    }

    
    private static final int MAX_THREAD_POOL_SIZE = 1;
    
    private static Log log = LogFactory.getLog(KafkaSubscriber.class);
    private static JsonEventParser eventParser = new JsonEventParser();
    
    private KafkaConsumer<Long, String> consumer;
    private Executor executor = null;
    private boolean doesPollFromBeginning = true;
    private long beginTime = 0;
}
