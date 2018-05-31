package databus.network.kafka;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import databus.core.Event;
import databus.network.EventParser;
import databus.network.AbstractSubscriber;
import databus.network.JsonEventParser;
import databus.network.Transporter;
import databus.util.Helper;

public class KafkaSubscriber extends AbstractSubscriber {
    
    public KafkaSubscriber() {
        super();
    }

    public void setConfigFile(String configFile) {
        Properties properties = Helper.loadProperties(configFile);

        if (null == properties.getProperty("group.id")) {
            properties.setProperty("group.id",
                                   "default-" + Math.round(Math.random()*1000000));
        }
        properties.setProperty("key.deserializer",
                               "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                               "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);
    }

    public void setEventParser(EventParser eventParser) {
        this.eventParser = eventParser;
    }

    public void setPollingTimeout(long pollingTimeout) {
        this.pollingTimeout = pollingTimeout;
    }

    @Override
    protected Transporter createTransporter() {
        return new PollingTransporter();
    }

    private final static Log log = LogFactory.getLog(KafkaSubscriber.class);

    private final PositionsCache positionsCache = new PositionsCache(1);

    private KafkaConsumer<String, String> consumer;
    private EventParser eventParser = new JsonEventParser();
    private long pollingTimeout = 2000;


    private class PollingTransporter implements Transporter {

        public PollingTransporter() {
        }

        @Override
        public void initialize() {

            consumer.subscribe(receiversMap.keySet(), new AutoRebalanceListener(consumer));
            KafkaHelper.seekRightPositions( consumer, consumer.assignment());
        }

        @Override
        public void runOnce() throws Exception {
            ConsumerRecords<String, String> records = consumer.poll(pollingTimeout);
            if ((null!=records) && (!records.isEmpty())) {               
                for (ConsumerRecord<String, String> r : records) {
                    String topic = r.topic();
                    int partition = r.partition();
                    long offset = r.offset();
                    String key = r.key();
                    String logPrefix = topic+ "   " +key + " (" + partition + ", " + offset + ")";
                    if (offset <= positionsCache.get(topic, partition)) {
                        log.warn(logPrefix + " is processed ahead : " + r.value());
                        continue;
                    } else {
                        positionsCache.set(topic, partition, offset);
                    }
                    
                    Event event = eventParser.toEvent(topic, key, r.value());
                    if (null == event) {
                        log.error("value can not be parser as an event " + logPrefix+ " : " +
                                  r.value());
                        continue;
                    } 
                    log.info(logPrefix + " : " + event.toString());
                    receive(topic, event);
                }
            }            
        }

        @Override
        public void processFinally() {            
        }

        @Override
        public void processException(Exception e) {
            log.warn("Catch Exception"+e.getClass().getName(), e);            
        }

        @Override
        public void stop(Thread owner) {
            while (owner.isAlive()) {
                log.info("Wake up consumer!");
                consumer.wakeup();
                log.info("Waiting consumer to finish!");
                try {
                    owner.join(1000);
                } catch (InterruptedException e) {
                    //do nothing
                }                
            }
        }

        @Override
        public void close() {
            consumer.close();
        }
    }
}
