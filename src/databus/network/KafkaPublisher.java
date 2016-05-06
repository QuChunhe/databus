package databus.network;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import databus.core.Event;

public class KafkaPublisher implements Publisher{    

    public KafkaPublisher() {
        super();        
    }

    @Override
    public void initialize(Properties properties) {
        String servers = properties.getProperty("kafka.servers");
        String acks = properties.getProperty("kafka.acks");
        String localHost = properties.getProperty("kafka.localAddress").trim();
        if ((null!=localHost) && (localHost.length()!=0)) {
            try {
                localAddress = InetAddress.getByName(localHost);
            } catch (UnknownHostException e) {
                log.error("kafka.localAddress has illegal value " + localHost, e);
                System.exit(1);
            }
        }
       
        Map<String, Object> config = new HashMap<String, Object>(6);
        config.put("bootstrap.servers", servers);
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
        event.ipAddress(localAddress);
        String topic = getKafcaTopic(event.topic().replace('/', '-'));
        String value = eventParser.toString(event);
        ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topic, time, value);
        producer.send(record);         
    }
    
    @Override
    public void stop() {
        producer.close();        
    }
    
    private String getKafcaTopic(String eventTopic) {
        return null==localAddress ? eventTopic : localAddress.getHostAddress()+eventTopic;
    }
    
    private static Log log = LogFactory.getLog(KafkaPublisher.class);
    private static EventParser eventParser = new EventParser();
    
    private KafkaProducer<Long, String> producer;
    private InetAddress localAddress = null;
}
