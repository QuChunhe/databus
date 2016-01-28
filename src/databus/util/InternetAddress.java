package databus.util;

public class InternetAddress {
    
    public InternetAddress(String ipAddress, int port) {
        this.ipAddress = normalize(ipAddress);
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
    
    public boolean isValid() {
        return (null!=ipAddress) && (port>0);
    }
    
    private String normalize(String rowIpAddress) {        
        String[] parts = rowIpAddress.split("\\.");        
        if (parts.length != 4) {
            return null;
        }
        String address = "";
        for(int i=0; i<4; i++) {
           int n = Integer.parseInt(parts[i]);
           if ((n>=0) && (n<=255)) {
               address += n;
           } else {
               return null;
           }
           if (i < 3) {
               address += ".";
           }
        }
        return address;
    }

    private String ipAddress;
    private int port;
}
