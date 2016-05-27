package databus.network;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import databus.core.Event;
import databus.util.Helper;

public class KafkaPublisher implements Publisher{    

    public KafkaPublisher() {
        super();        
    }

    @Override
    public void initialize(Properties properties) {
        String kafkaAddressValue = properties.getProperty("kafka.server").trim();
        kafkaAddress = Helper.normalizeSocketAddress(kafkaAddressValue);
        if (null == kafkaAddress) {
            log.error("kafka.server has illegal value "+kafkaAddressValue);
        }
        String acks = properties.getProperty("kafka.acks");
               
        Map<String, Object> config = new HashMap<String, Object>(6);
        config.put("bootstrap.servers", kafkaAddress);
        config.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("compression.type", "gzip");
        config.put("request.required.acks", acks);
        config.put("block.on.buffer.full", true);
        producer = new KafkaProducer<Long, String>(config);        
    }

    @Override
    public void publish(Event event) {
        Long time = System.currentTimeMillis();
        String topic = SPECIAL_CHARACTER.matcher(kafkaAddress+event.topic())
                                        .replaceAll("-");
        String value = eventParser.toString(event);
        log.info(topic+" : "+value);
        ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topic, time, value);
        producer.send(record);         
    }
    
    @Override
    public void stop() {
        producer.close();        
    }
    
    private static final Pattern SPECIAL_CHARACTER = Pattern.compile("_|:|/");
    
    private static Log log = LogFactory.getLog(KafkaPublisher.class);
    private static JsonEventParser eventParser = new JsonEventParser();
    
    private KafkaProducer<Long, String> producer;
    private String kafkaAddress;
    
}
