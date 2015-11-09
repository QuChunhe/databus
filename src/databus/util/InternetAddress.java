package databus.util;

import java.io.Serializable;

public class InternetAddress implements Serializable {

    private static final long serialVersionUID = 8532528699989931341L;
    
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
           if (ipAddress.equals(o.ipAddress) && (port == o.port)) {
               return true;
           }
       }
       return false;
    }

    @Override
    public String toString() {
        return ipAddress+":"+port;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    private String ipAddress;
    private int port;
}
