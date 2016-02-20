package databus.network;

import java.net.SocketAddress;

import databus.core.Clearable;
import io.netty.buffer.ByteBuf;

public class Task implements Clearable{    
    
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

    @Override
    public void clear() {
        buffer = null;
        address = null;        
    }    
    
    private SocketAddress address;
    private ByteBuf buffer;

}
