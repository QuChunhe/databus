package databus.network;


public class Task {    
    
    public Task(InternetAddress address, String message) {
        this.address = address;
        this.message = message;
    }
    
    public String ipAddress() {
        return address.ipAddress();
    }
    
    public int port() {
        return address.port();
    }
    
    public String message() {
        return message;
    }
    
    private InternetAddress address;
    private String message;

}
