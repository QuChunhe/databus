package databus.core;

public class IpAddress {    
    
    public IpAddress(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String ip() {
        return ip;
    }
    
    public int port() {
        return port;
    }
    
    private String ip;
    private int port;

}
