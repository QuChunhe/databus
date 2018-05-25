package databus.receiver.kafka;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import databus.core.Receiver;
import databus.util.Helper;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
public abstract class KafkaRecevier implements Receiver {
    public KafkaRecevier() {
    }

    @Override
    public void close() throws IOException {
        kafkaProducer.close();
    }

    public void setConfigFile(String configFile) {
        Properties config = Helper.loadProperties(configFile);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        if (null == config.getProperty("compression.type")) {
            config.put("compression.type", "gzip");
        }
        kafkaProducer = new KafkaProducer<>(config);

    }

    public void setKafkaProducer(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    protected void send(String topic, String key, String value) {
        long startTime = System.currentTimeMillis();
        kafkaProducer.send(new ProducerRecord<>(topic, key, value),
                           new Callback() {
                               @Override
                               public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                   long duration = System.currentTimeMillis() - startTime;
                                   if (null != recordMetadata) {
                                       log.info(duration+"ms " + recordMetadata.toString());
                                   } else {
                                       log.error(duration+"ms " + topic + " " + key + " : " +value, e);
                                   }
                               }
                           });
    }

    private final static Log log = LogFactory.getLog(KafkaRecevier.class);

    private KafkaProducer<String, String> kafkaProducer;
}
