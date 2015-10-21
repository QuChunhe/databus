package databus.network;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client {
    
    public void init() {
        EventLoopGroup group = new NioEventLoopGroup();
        ClientHandler handler = new ClientHandler();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .handler(handler);
        } finally {
            group.shutdownGracefully();
        }
    }

}
