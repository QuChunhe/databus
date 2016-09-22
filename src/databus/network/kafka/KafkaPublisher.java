package databus.network.kafka;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import databus.core.Event;
import databus.core.Publisher;
import databus.network.JsonEventParser;
import databus.util.Helper;

public class KafkaPublisher implements Publisher {    

    public KafkaPublisher() {
        super();
    }

    @Override
    public void initialize(Properties properties) {
        String kafkaServerValue = properties.getProperty("kafka.server").trim();
        String kafkaServer = Helper.normalizeSocketAddress(kafkaServerValue);
        if (null == kafkaServer) {
            log.error("kafka.server has illegal value "+kafkaServerValue);
            System.exit(1);
        }
        String acks = properties.getProperty("kafka.acks", "1");               
        Map<String, Object> config = new HashMap<String, Object>(6);
        config.put("bootstrap.servers", kafkaServer);
        config.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("compression.type", "gzip");
        config.put("acks", acks);
        config.put("max.block.ms", 60000);
        producer = new KafkaProducer<Long, String>(config);
    }

    @Override
    public void publish(Event event) {
        event.ipAddress(kafkaInetAddress);
        Long time = System.currentTimeMillis();
        String topic =  SPECIAL_CHARACTER.matcher(event.topic()).replaceAll("-");
        event.topic(null);
        String value = eventParser.toString(event);
        log.info(time + " " + topic +" : " +value);
        ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topic, time, value);
        producer.send(record, new LogCallback(topic, time, value));         
    }
    
    @Override
    public void stop() {
        producer.close();        
    }
    
    private static final Pattern SPECIAL_CHARACTER = Pattern.compile("_|:|/|\\.");
    
    private static Log log = LogFactory.getLog(KafkaPublisher.class);
    private static JsonEventParser eventParser = new JsonEventParser();
    
    private KafkaProducer<Long, String> producer;
    private InetAddress kafkaInetAddress;
    
    private static class LogCallback implements Callback {        
        
        public LogCallback(String topic, long key, String value) {
            this.key = key;
            this.topic = topic;
            this.value = value;
        }
        
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (null != metadata) {
                log.info(key + " " + topic + " (" + metadata.partition() + "," + 
                         metadata.offset() + ") : " + value);
            } else {
                log.error(key + " " + topic + " : " +value, exception);
            }
        }        

        private long key;        
        private String value;
        private String topic;
    }    
}
