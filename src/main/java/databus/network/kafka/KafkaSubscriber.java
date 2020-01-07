package databus.network.kafka;

import java.time.Duration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.kafka.clients.consumer.*;

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
        this.pollingTimeout = Duration.ofMillis(pollingTimeout);
    }

    public void setOffsetCommitter(OffsetCommitter<String, String> offsetCommitter) {
        this.offsetCommitter = offsetCommitter;
    }

    @Override
    protected Transporter createTransporter() {
        return new PollingTransporter();
    }

    private final static Log log = LogFactory.getLog(KafkaSubscriber.class);

    private KafkaConsumer<String, String> consumer;
    private EventParser eventParser = new JsonEventParser();
    private Duration pollingTimeout = Duration.ofMillis(2000);
    private OffsetCommitter<String, String> offsetCommitter = new AutoOffsetCommitter<>();


    private class PollingTransporter implements Transporter {

        public PollingTransporter() {
        }

        @Override
        public void initialize() {
            consumer.subscribe(receiversMap.keySet(), offsetCommitter);
            offsetCommitter.initialize(consumer);
        }

        @Override
        public void runOnce() throws Exception {
            ConsumerRecords<String, String> records = consumer.poll(pollingTimeout);
            if ((null!=records) && (!records.isEmpty())) {
                offsetCommitter.afterPolling();
                //ensure to process every record
                for (ConsumerRecord<String, String> r : records) {
                    String topic = r.topic();
                    int partition = r.partition();
                    long offset = r.offset();
                    String key = r.key();

                    if (!offsetCommitter.beforeProcessing(topic, partition, offset)) {
                        log.error("Has received "+topic+" ("+partition+", "+offset+")");
                        continue;
                    }

                    Event event = eventParser.toEvent(topic, key, r.value());
                    if (null == event) {
                        log.error("value can not be parser as an event : " + r.value());
                        continue;
                    } 
                    log.info(topic+ "   " +key + " (" + partition + ", " + offset + ")" + " : " +
                             event.toString());
                    receive(topic, event);
                    offsetCommitter.afterProcessing(topic, partition, offset);
                }
                offsetCommitter.beforePolling();
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
            log.info("Wake up consumer!");
            consumer.wakeup();
        }

        @Override
        public void close() {
            try {
                offsetCommitter.close();
            }catch (Exception e) {
                log.error("Can not commit offset", e);
            } finally {
                consumer.close();
            }
        }
    }

}