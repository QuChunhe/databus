package databus.receiver.relay;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import databus.core.Receiver;
import databus.core.Event;
import databus.network.JsonEventParser;
import databus.util.KafkaFactory;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
public class KafkaRelayer implements Receiver {
    public KafkaRelayer() {
    }

    @Override
    public void receive(Event event) {
        EventWrapper eventWrapper = eventTransformer.transform(event);
        if (null == eventWrapper) { ;
            return;
        }
        send(eventWrapper.getTopic(), eventWrapper.getKey(), eventWrapper.getEvent());
    }

    @Override
    public void close() throws IOException {
        kafkaProducer.close();
    }

    public void setConfigFile(String configFile) {
        kafkaProducer = KafkaFactory.createKafkaProducer(configFile);
    }

    public void setKafkaProducer(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void setEventParser(JsonEventParser eventParser) {
        this.eventParser = eventParser;
    }

    public void setEventTransformer(EventTransformer eventTransformer) {
        this.eventTransformer = eventTransformer;
    }

    protected void send(String topic, Event event) {
        send(topic, eventParser.toKey(event), eventParser.toMessage(event));
    }

    protected void send(String topic, String key, Event event) {
        if (null == key) {
            key = eventParser.toKey(event);
        }
        send(topic, key, eventParser.toMessage(event));
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
    private EventTransformer eventTransformer;
}
