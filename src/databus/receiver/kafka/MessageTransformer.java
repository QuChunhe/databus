package databus.receiver.kafka;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
interface MessageTransformer {

    String topic(String redisMessage);

    String value(String redisMessage);

    String key(String redisMessage);
}
