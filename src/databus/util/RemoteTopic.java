package databus.util;

import java.io.Serializable;

public class RemoteTopic implements Serializable{

    private static final long serialVersionUID = -7320388531653089553L;
    
    public RemoteTopic(InternetAddress remoteAddress, String topic) {
        this.remoteAddress = remoteAddress;
        this.topic = topic.toUpperCase();
        name = remoteAddress.toString()+"/"+topic;
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
        return name;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    private InternetAddress remoteAddress;
    private String topic;
    private String name;
}
