package databus.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class IpTopic {
    
    public IpTopic(InetAddress ipAddress, String topic) {
        this.ipAddress = ipAddress;
        this.topic = topic;
        hashCode = (ipAddress.getHostAddress()+"/"+topic).hashCode();
    }
    
    public IpTopic(String hostName, String topic) throws UnknownHostException {
        this(InetAddress.getByName(hostName), topic);
    }

    public InetAddress iAddress() {
        return ipAddress;
    }
    
    public String topic() {
        return topic;
    }    
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof IpTopic) {
            IpTopic o = (IpTopic) other;
            return ipAddress.equals(o.ipAddress) && topic.equals(o.topic);            
        }
        return false;
    }    
    
    @Override
    public String toString() {
        return ipAddress+"/"+topic;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }   
    
    protected int hashCode;
    
    private InetAddress ipAddress;
    private String topic;
    
}
