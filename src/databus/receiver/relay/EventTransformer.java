package databus.receiver.relay;

import databus.core.Event;

/**
 * Created by Qu Chunhe on 2018-05-24.
 */
public interface EventTransformer {

    EventWrapper transform(Event event);

}
