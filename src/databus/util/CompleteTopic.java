package databus.util;

public class CompleteTopic extends RemoteTopic {
    
    public CompleteTopic(InternetAddress remoteAddress, String topic) {
        super(remoteAddress.ipAddress(), topic);
        port = remoteAddress.port();
        hashCode = toString().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof CompleteTopic) {
            CompleteTopic o = (CompleteTopic) other;
            return super.equals(o) && (port==o.port);            
        }
        return false;
    }

    @Override
    public String toString() {
        return ipAddress()+":"+port+"/"+topic();
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return super.hashCode();
    }
    
    public int port() {
        return port;
    }
    
    public InternetAddress internetAddress() {
        return new InternetAddress(ipAddress(), port);
    }
    
    private int port;

}
