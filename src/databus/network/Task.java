package databus.network;

public class Task {    
    
    public Task(String ip, int port, String message) {
        this.ip = ip;
        this.port = port;
        this.message = message;
    }
    
    public String ip() {
        return ip;
    }
    
    public int port() {
        return port;
    }
    
    public String message() {
        return message;
    }
    
    private String ip;
    private int port;
    private String message;

}
