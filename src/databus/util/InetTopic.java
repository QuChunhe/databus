package databus.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class InetTopic {
    
    public InetTopic(InetAddress ipAddress, String topic) {
        this.ipAddress = ipAddress;
        this.topic = topic;
        hashCode = (ipAddress.toString()+"/"+topic).hashCode();
    }
    
    public InetTopic(String hostName, String topic) throws UnknownHostException {
        this(InetAddress.getByName(hostName), topic);
    }

    public InetAddress ipAddress() {
        return ipAddress;
    }
    
    public String topic() {
        return topic;
    }    
    
    @Override
    public boolean equals(Object other) {
        if ((null!=other) && (other instanceof InetTopic)) {
            InetTopic o = (InetTopic) other;
            return ipAddress.equals(o.ipAddress) && topic.equals(o.topic);                                  
        }
        return false;
    }    
    
    @Override
    public String toString() {
        return ipAddress.toString()+"/"+topic;
    }

    @Override
    public int hashCode() {
        return topic.hashCode();
    }   
    
    protected final int hashCode;
    
    private final InetAddress ipAddress;
    private final String topic;
    
}
