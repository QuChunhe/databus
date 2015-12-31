package databus.event;

import databus.core.Event;

public interface RedisEvent extends Event {
    static public enum Type {
        LIST_MESSAGING, KEYSPACE_NOTIFICATION, Pub_NOTIFICATION
    }

    public String key();
}
