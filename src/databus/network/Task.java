package databus.network;

import java.net.SocketAddress;

import io.netty.buffer.ByteBuf;

public class Task {    
    
    public Task(SocketAddress address, ByteBuf buffer) {
        this.address = address;
        this.buffer = buffer;
    }
    
    public SocketAddress address() {
        return address;
    }

    
    public ByteBuf buffer() {
        return buffer;
    }   

    private SocketAddress address;
    private ByteBuf buffer;
}
