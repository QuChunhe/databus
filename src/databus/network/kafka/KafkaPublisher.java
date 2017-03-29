package databus.network.kafka;

import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import databus.core.Event;
import databus.network.JsonEventParser;
import databus.util.Helper;
import databus.network.AbstractPublisher;

public class KafkaPublisher extends AbstractPublisher {

    public KafkaPublisher() {
        super();
    }

    public void setConfigFileName(String configFileName) {
        Properties properties = Helper.loadProperties(configFileName);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("compression.type", "gzip");

        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void publish(Event event) {
        Long time = System.currentTimeMillis();
        String topic =  SPECIAL_CHARACTER.matcher(event.topic()).replaceAll("-");
        event.topic(null);
        String value = eventParser.toString(event);
        log.info(time + " " + topic +" : " +value);
        ProducerRecord<Long, String> record = new ProducerRecord<>(topic, time, value);
        producer.send(record, new LogCallback(topic, time, value));         
    }
    
    @Override
    public void stop() {
        super.stop();
        producer.close();        
    }

    private final static Log log = LogFactory.getLog(KafkaPublisher.class);
    private final static Pattern SPECIAL_CHARACTER = Pattern.compile("_|:|/|\\.");
    private final static JsonEventParser eventParser = new JsonEventParser();
    
    private KafkaProducer<Long, String> producer;

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
