package databus.core;

public interface Event {
    
    public static enum Source {REDIS, MYSQL, MANAGEMENT}

    public Source source();
    
    public String topic();
    
    public long time();
    
    public String type();
}
