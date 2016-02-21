package databus.network;

import java.net.SocketAddress;

public class Task {    
    
    public Task(SocketAddress socketAddress, String message) {
        this.socketAddress = socketAddress;
        this.message = message;
    }
    
    public SocketAddress socketAddress() {
        return socketAddress;
    }
    
    public String message() {
        return message;
    }
    
    private SocketAddress socketAddress;
    private String message;
}
