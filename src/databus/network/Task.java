package databus.network;

import databus.util.InternetAddress;

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
    
    @Override
    public String toString() {
        return address.toString()+" : "+message;
    }



    private InternetAddress address;
    private String message;

}
