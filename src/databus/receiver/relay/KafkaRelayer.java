package databus.receiver.relay;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import databus.core.Receiver;
import databus.core.Event;
import databus.network.JsonEventParser;
import databus.util.Helper;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
public abstract class KafkaRelayer implements Receiver {
    public KafkaRelayer() {
    }

    @Override
    public void close() throws IOException {
        kafkaProducer.close();
    }

    public void setConfigFile(String configFile) {
        Properties properties = Helper.loadProperties(configFile);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (null == properties.getProperty("compression.type")) {
            properties.put("compression.type", "gzip");
        }

        kafkaProducer = new KafkaProducer<>(properties);
    }

    public void setKafkaProducer(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void setEventParser(JsonEventParser eventParser) {
        this.eventParser = eventParser;
    }

    protected void send(String topic, Event event) {
        send(topic, eventParser.toKey(event), eventParser.toMessage(event));
    }

    protected void send(String topic, String key, String value) {
        kafkaProducer.send(new ProducerRecord<>(topic, key, value),
                           new Callback() {
                               @Override
                               public void onCompletion(RecordMetadata metadata, Exception e) {
                                   if (null != metadata) {
                                       log.info(topic + " (" + metadata.partition() + "," +
                                                metadata.offset() + ")   "  + key + " : " + value);
                                   } else {
                                       log.error( topic + " " + key + " : " +value, e);
                                   }
                               }
                           });
    }

    private final static Log log = LogFactory.getLog(KafkaRelayer.class);

    private KafkaProducer<String, String> kafkaProducer;
    private JsonEventParser eventParser = new JsonEventParser();
}
