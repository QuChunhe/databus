package databus.network;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import databus.util.Helper;

public class KafkaHelper {
    
    public static Executor loadExecutor(Properties properties, int defaultMaxThreadPoolSize) {
        String maxThreadPoolSizeValue = properties.getProperty("kafka.maxThreadPoolSize");
        int maxThreadPoolSize = null==maxThreadPoolSizeValue ? 
                                defaultMaxThreadPoolSize : 
                                Integer.parseInt(maxThreadPoolSizeValue);
        Executor executor = null;
        if (maxThreadPoolSize > 0) {
            String taskCapacityValue = properties.getProperty("kafka.taskCapacity");
            int taskCapacity = null==taskCapacityValue ? 
                               DEFAULT_TASK_CAPACITY  : 
                               Integer.parseInt(taskCapacityValue);
            executor = new ThreadPoolExecutor(1, maxThreadPoolSize, 
                                              100, TimeUnit.SECONDS, 
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
    
    private static final int DEFAULT_TASK_CAPACITY = 1000;

}
