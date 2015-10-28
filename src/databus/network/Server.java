package databus.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Subscriber;
import databus.util.Configuration;
import databus.util.InternetAddress;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class Server implements Runnable, Startable{    
    
    public Server(Subscriber subscriber) {
        this.subscriber = subscriber;
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
    }
    
    @Override
    public Thread start() {
        if (null == thread) {
            thread = new Thread(this, "Databus Server");
            
        } 
        if (thread.getState() == Thread.State.NEW) {
            thread.start();
        }        
        return thread;
    }

    @Override
    public void run() { 
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
            log.error("Server Thread is interrupted", e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public void stop() {        
        if (null != thread) {
            thread.interrupt();
        }
    } 
    
    public InternetAddress getListeningAddress() {
        return localAddress;
    }
    
    private static Log log = LogFactory.getLog(Server.class);
    
    private Subscriber subscriber;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private InternetAddress localAddress;
    private Thread thread = null;

}
