package databus.network;

import databus.core.Event;

/**
 * Created by Qu Chunhe on 2018-05-31.
 */
public interface KeyMapper {

    String toKey(Event event);

    Class<? extends Event> toEventClass(String key);
}
