package databus.network;

public class InternetAddress {
    public InternetAddress(String ipAddress, int port) {
        super();
        this.ipAddress = ipAddress;
        this.port = port;
    }
    
    public String ipAddress() {
        return ipAddress;
    }
    
    public int port() {
        return port;
    }
    
    private String ipAddress;
    private int port;
}
