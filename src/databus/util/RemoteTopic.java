package databus.util;

public class RemoteTopic {
    
    public RemoteTopic(InternetAddress remoteAddress, String topic) {
        this.remoteAddress = remoteAddress;
        this.topic = topic;
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
    
    private InternetAddress remoteAddress;
    private String topic;
}
