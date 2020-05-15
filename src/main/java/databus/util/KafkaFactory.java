package databus.util;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Created by Qu Chunhe on 2020-05-15.
 */
public class KafkaFactory {

    public static KafkaProducer<String, String> createKafkaProducer(String configFile) {
        Properties properties = Helper.loadProperties(configFile);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (null == properties.getProperty("compression.type")) {
            properties.put("compression.type", "gzip");
        }

        if (null == properties.getProperty("partitioner.class")) {
            properties.put("partitioner.class", "databus.network.kafka.RoundRobinPartitioner");
        }

        return new KafkaProducer<>(properties);
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(String configFile) {
        Properties properties = Helper.loadProperties(configFile);

        if (null == properties.getProperty("group.id")) {
            properties.setProperty("group.id",
                    "default-" + Math.round(Math.random()*1000000));
        }
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(properties);
    }
}
