package databus.network.kafka;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import databus.util.Helper;

public class KafkaHelper {
    
    public static ExecutorService loadExecutor(Properties properties, 
                                               int defaultMaxThreadPoolSize) {
        String maxThreadPoolSizeValue = properties.getProperty("kafka.maxWorkerThreadPoolSize");
        int maxThreadPoolSize = null==maxThreadPoolSizeValue ? 
                                defaultMaxThreadPoolSize : 
                                Integer.parseInt(maxThreadPoolSizeValue);
        ExecutorService executor = null;
        if (maxThreadPoolSize > 0) {
            String taskCapacityValue = properties.getProperty("kafka.taskCapacity");
            int taskCapacity = null==taskCapacityValue ? 
                               DEFAULT_TASK_CAPACITY  : 
                               Integer.parseInt(taskCapacityValue);
            executor = new ThreadPoolExecutor(1, maxThreadPoolSize, 
                                              30, TimeUnit.SECONDS, 
                                              new LinkedBlockingQueue<Runnable>(taskCapacity),
                                              Executors.defaultThreadFactory(),
                                              new ThreadPoolExecutor.CallerRunsPolicy());
        }
        return executor;
    }
    
    public static String splitSocketAddress(String remoteTopic) {
        int index = remoteTopic.indexOf('/');
        if (index < 1) {
            return null;
        }
        return Helper.normalizeSocketAddress(remoteTopic.substring(0, index));
    }
    
    public static String splitTopic(String remoteTopic) {
        int index = remoteTopic.indexOf('/');
        if (index < 1) {
            return null;
        }
        if (index == remoteTopic.length()) {
            return null;
        }
        return remoteTopic.substring(index+1);
    }
    
    public static void seekRightPositions(KafkaConsumer<Long, String> consumer, 
                                          Collection<TopicPartition> partitions) {
        PositionsCache cache = new PositionsCache();
        HashSet<TopicPartition> topicPartitions = null;
        for(TopicPartition p : partitions) {
            long offset = cache.get(p.topic(), p.partition());
            if (offset < 0) {
                if (null == topicPartitions) {
                    topicPartitions = new HashSet<TopicPartition>();
                }
                topicPartitions.add(p);
            } else {
                //if the position is out of partition range, 
                //the offset depends on the topic and the value of auto.offset.reset.
                consumer.seek(p, offset+1); 
            }             
        }
        if (null != topicPartitions) {
            consumer.seekToEnd(topicPartitions);
        }
    }
    
    public static String getAliasKafkaTopic(String aliasServer, String kafkaTopic) {
        return kafkaTopic.startsWith("-") ? aliasServer + kafkaTopic 
                                          : aliasServer + "-" + kafkaTopic;
    }
    
    private static final int DEFAULT_TASK_CAPACITY = 10;
}
