package databus.util;

public class InternetAddress {
    
    public InternetAddress(String ipAddress, int port) {
        this.ipAddress = ipAddress.trim();
        this.port = port;
    }
    
    public String ipAddress() {
        return ipAddress;
    }
    
    public int port() {
        return port;
    }    
    
    @Override
    public boolean equals(Object other) {
       if(other instanceof InternetAddress) {
           InternetAddress o = (InternetAddress) other;
           return ipAddress.equals(o.ipAddress);
       }
       return false;
    }

    @Override
    public String toString() {
        return ipAddress+":"+port;
    }

    @Override
    public int hashCode() {
        return ipAddress.hashCode();
    }

    private String ipAddress;
    private int port;
}
