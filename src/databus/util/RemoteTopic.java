package databus.util;

public class RemoteTopic {
    
    public RemoteTopic(String ipAddress, String topic) {
        this.ipAddress = ipAddress;
        this.topic = topic;
        hashCode = (ipAddress+"/"+topic).hashCode();
    }

    public String ipAddress() {
        return ipAddress;
    }
    
    public String topic() {
        return topic;
    }    
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof RemoteTopic) {
            RemoteTopic o = (RemoteTopic) other;
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
    
    private String ipAddress;
    private String topic;
    
}
