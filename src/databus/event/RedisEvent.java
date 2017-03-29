package databus.event;

import databus.core.Event;

public interface RedisEvent extends Event {
    enum Type {
        LIST_MESSAGING, KEYSPACE_NOTIFICATION, PUB_NOTIFICATION
    }

    String key();
}
