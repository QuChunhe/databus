package databus.core;

public interface Event {
    public static enum Source {REDIS, MYSQL}

    public Source source();
    
    public String topic();
    
    public long time();
    
}
