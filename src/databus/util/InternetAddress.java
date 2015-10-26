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



    private String ipAddress;
    private int port;
}
