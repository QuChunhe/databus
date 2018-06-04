package databus.network.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

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

public class KafkaPublisher extends AbstractPublisher implements Closeable {

    public KafkaPublisher() {
        super();
    }

    public void setConfigFile(String configFile) {
        Properties properties = Helper.loadProperties(configFile);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (null == properties.getProperty("compression.type")) {
            properties.put("compression.type", "gzip");
        }

        if (null == properties.getProperty("partitioner.class")) {
            properties.put("partitioner.class", "databus.network.kafka.RoundRobinPartitioner");
        }

        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void publish(Event event) {
        publish(topic, event);
    }

    @Override
    public void publish(String topic, Event event) {
        if (null == topic) {
            topic = this.topic;
        }
        if (null == topic) {
            log.error("topic is null!");
            return;
        }

        String value = eventParser.toMessage(event);
        String key = eventParser.toKey(event);
        log.info(topic +" -- " + key + " : " +value);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, new LogCallback(topic, key, value));
    }

    @Override
    public void stop() {
        super.stop();
        try {
            close();
        } catch (IOException e) {
            //do nothing
        }
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setEventParser(JsonEventParser eventParser) {
        this.eventParser = eventParser;
    }

    public void setProducer(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    private final static Log log = LogFactory.getLog(KafkaPublisher.class);

    private JsonEventParser eventParser = new JsonEventParser();
    private KafkaProducer<String, String> producer;
    private String topic = null;

    private static class LogCallback implements Callback {        
        
        public LogCallback(String topic, String key, String value) {
            this.key = key;
            this.topic = topic;
            this.value = value;
        }
        
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (null != metadata) {
                log.info(topic + " (" + metadata.partition() + "," +
                        metadata.offset() + ")   "  + key + " : " + value);
            } else {
                log.error(topic + " " + key + " : " +value, exception);
            }
        }        

        private final String key;
        private final String value;
        private final String topic;
    }    
}