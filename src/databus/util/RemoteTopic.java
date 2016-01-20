package databus.util;

public class RemoteTopic {
    
    public RemoteTopic(InternetAddress remoteAddress, String topic) {
        this.remoteAddress = remoteAddress;
        this.topic = topic;
        hasCode = (remoteAddress.ipAddress()+topic).hashCode();
    }
    
    public RemoteTopic(String host, int port, String topic) {
        this(new InternetAddress(host, port), topic);
    }
    
    public InternetAddress remoteAddress() {
        return remoteAddress;
    }
    
    public String topic() {
        return topic;
    }    
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof RemoteTopic) {
            RemoteTopic o = (RemoteTopic) other;
            return remoteAddress.equals(o.remoteAddress) && topic.equals(o.topic);            
        }
        return false;
    }    
    
    @Override
    public String toString() {
        return remoteAddress.toString()+"/"+topic;
    }

    @Override
    public int hashCode() {
        return hasCode;
    }

    private InternetAddress remoteAddress;
    private String topic;
    private int hasCode;
}
