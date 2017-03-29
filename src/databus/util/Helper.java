package databus.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Helper {
    
    public static long ipToInt(String ipAddr) throws UnknownHostException, DataFormatException {
        InetAddress inetAddress = InetAddress.getByName(ipAddr);
        byte[] parts = inetAddress.getAddress();
        if (parts.length != 4) {
            throw new DataFormatException(ipAddr+" length does't equal to 4");
        }
        long ip = 0;
        ip |= Byte.toUnsignedLong(parts[0]) << 24;
        ip |= Byte.toUnsignedLong(parts[1]) << 16;
        ip |= Byte.toUnsignedLong(parts[2]) << 8;
        ip |= Byte.toUnsignedLong(parts[3]);
     
        return ip;
    }

    public static String replaceEscapeString(String sql) {
        if (null == sql) {
            return "";
        }
        String replace = BSLASH_PATTERN.matcher(sql).replaceAll("\\\\\\\\");
        replace = QUOTE_PATTERN.matcher(replace).replaceAll("\\\\'");
        return replace;
    }
    
    public static String normalizeSocketAddress(String address) {
        String[] parts = address.split(":");
        if (parts.length != 2) {
            return null;
        }
        String normalizedAddress = null;
        try {
            int port = Integer.parseInt(parts[1]);
            if (port >= (1<<16)) {
                return null;
            }
            InetAddress inetAddress = InetAddress.getByName(parts[0]);
            normalizedAddress = inetAddress.getHostAddress() + ":" + port;
        } catch(Exception e) {
            // do nothing
        }
        return normalizedAddress;        
    }
    
    public static String toAlias(String name) {
        return name.replace('.', '_')
                   .replace('/', '-')
                   .replace(':', '-');
    }
    
    public static ExecutorService loadExecutor(Properties properties, 
                                               int defaultMaxThreadPoolSize) {
        String maxThreadPoolSizeValue = properties.getProperty("maxWorkerThreadPoolSize");
        int maxThreadPoolSize = null==maxThreadPoolSizeValue ? 
                                defaultMaxThreadPoolSize : 
                                Integer.parseInt(maxThreadPoolSizeValue);
        ExecutorService executor = null;
        if (maxThreadPoolSize > 0) {
            String taskCapacityValue = properties.getProperty("taskCapacity");
            int taskCapacity = null==taskCapacityValue ? 
                               DEFAULT_TASK_CAPACITY  : 
                               Integer.parseInt(taskCapacityValue);
            if (taskCapacity < 1) {
                taskCapacity = DEFAULT_TASK_CAPACITY;
            }
            executor = loadExecutor(1, maxThreadPoolSize, 30, taskCapacity);
        }
        return executor;
    }

    public static ExecutorService loadExecutor(int corePoolSize, int maximumPoolSize,
                                               long keepAliveSeconds, int taskQueueCapacity) {
        return new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
                                      keepAliveSeconds, TimeUnit.SECONDS,
                                      new ArrayBlockingQueue<>(taskQueueCapacity),
                                      Executors.defaultThreadFactory(),
                                      new CallerWaitsPolicy());
    }

    public static String substring(String string, String splitter) {
        int position = string.indexOf(splitter);
        return position<0 ? null : string.substring(position+splitter.length());
    }

    public static Properties loadProperties(String file) {
        if ((null==file) || (file.length()==0)) {
            log.error("Configuration file is null!");
            System.exit(1);
        }
        Properties properties = new Properties();
        try {
            properties.load(Files.newBufferedReader(Paths.get(file)));
        } catch (IOException e) {
            log.error("Can not load properties file "+ file, e);
            System.exit(1);
        }
        return properties;
    }
    
    private final static Pattern BSLASH_PATTERN = Pattern.compile("\\\\");
    private final static Pattern QUOTE_PATTERN = Pattern.compile("\\'");
    private final static int DEFAULT_TASK_CAPACITY = 10;

    public static class CallerWaitsPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            for (;;) {
                try {
                    executor.getQueue().put(r);
                    return;
                } catch (InterruptedException e) {

                }
            }
        }
    }

    private final static Log log = LogFactory.getLog(Helper.class);
}
