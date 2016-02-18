package databus.network;

import databus.util.InternetAddress;
import io.netty.buffer.ByteBuf;

public class Task {    
    
    public Task(InternetAddress address, ByteBuf buffer) {
        this.address = address;
        this.buffer = buffer;
    }
    
    public String ipAddress() {
        return address.ipAddress();
    }
    
    public int port() {
        return address.port();
    }
    
    public ByteBuf buffer() {
        return buffer;
    }   

    private InternetAddress address;
    private ByteBuf buffer;
}
