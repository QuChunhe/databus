package databus.network;

import databus.core.Startable;
import databus.core.Subscriber;
import databus.util.Configuration;
import databus.util.InternetAddress;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class Server implements Startable{    
    
    public Server(Subscriber subscriber) {
        this.subscriber = subscriber;
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
    }
    
    @Override
    public void start() {
        ServerHandler childHandler = new ServerHandler(subscriber);
        
        InternetAddress address = 
                Configuration.instance().loadListeningAddress();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.option(ChannelOption.SO_BACKLOG, 1024)
                     .group(bossGroup, workerGroup)
                     .channel(NioServerSocketChannel.class)
                     .localAddress(address.ipAddress(), address.port())
                     .childHandler(childHandler);
            
            Channel channel = bootstrap.bind().sync().channel();
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            
            e.printStackTrace();
        } finally {
            stop();
        }
    }
    
    @Override
    public boolean isRunning() {
        boolean isShuttingDown = bossGroup.isShuttingDown() || 
                                 workerGroup.isShuttingDown();
        return !isShuttingDown;
    }

    @Override
    public void stop() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        
    } 
    
    public InternetAddress getListeningAddress() {
        return localAddress;
    }
    
    private Subscriber subscriber;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private InternetAddress localAddress;
}
