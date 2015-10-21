package databus.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class Server {    
    
    public Server(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }
    
    public void start() {
        ServerHandler childHandler = new ServerHandler();
        
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.option(ChannelOption.SO_BACKLOG, 1024)
                     .group(bossGroup, workerGroup)
                     .channel(NioServerSocketChannel.class)
                     .localAddress(ip, port)
                     .childHandler(childHandler);
            
            Channel channel = bootstrap.bind().sync().channel();
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }       
        
    }
    
    private String ip;
    private int port;

}
