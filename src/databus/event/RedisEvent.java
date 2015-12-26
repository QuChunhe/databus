package databus.event;

import databus.core.Event;

public interface RedisEvent extends Event {
    static public enum Type {
        LIST_MESSAGES, KEYSPACE_NOTIFICATION, Pub_SUB
    }

    public String key();
}
