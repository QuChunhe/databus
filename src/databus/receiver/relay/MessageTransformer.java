package databus.receiver.relay;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
interface MessageTransformer {

    String topic();

    String toMessage(String message);

}
