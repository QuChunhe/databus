package databus.util;

public class RemoteTopic {
    
    public RemoteTopic(InternetAddress remoteAddress, String topic) {
        this.remoteAddress = remoteAddress;
        this.topic = topic.toUpperCase();
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
            if (remoteAddress.equals(o.remoteAddress) && 
                    topic.equals(o.topic)) {
               return true; 
            }
        }
        return false;
    }    
    
    @Override
    public String toString() {
        return remoteAddress.toString()+"/"+topic.replace(":", "/");
    }

    @Override
    public int hashCode() {
        return (remoteAddress.ipAddress()+topic).hashCode();
    }

    private InternetAddress remoteAddress;
    private String topic;
}
