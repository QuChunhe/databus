package databus.core;

public interface Event{
    
    enum Source {REDIS, MYSQL}

    Source source();

    long time();
    
    Event time(long time);
    
    String type();
}
