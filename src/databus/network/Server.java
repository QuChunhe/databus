package databus.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.util.InternetAddress;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class Server implements Runnable, Startable{    
    
    public Server(InternetAddress localAddress) {
        this(localAddress,1);
    }
    
    public Server(InternetAddress localAddress, int workerPoolSize) {
        this.localAddress = localAddress;
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
    }
    
    @Override
    public Thread start() {
        if (null == thread) {
            thread = new Thread(this, "Databus Server");
            thread.start();
        }
        return thread;
    }

    @Override
    public void run() {        
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.option(ChannelOption.SO_BACKLOG, 1024)
                     .group(bossGroup, workerGroup)
                     .channel(NioServerSocketChannel.class)
                     .localAddress(localAddress.ipAddress(), 
                                   localAddress.port())
                     .childHandler(new ChannelInitializer<SocketChannel>(){

                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new ServerHandler(publisher, subscriber));
                        }
                         
                     });
            
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

    public Server setPublisher(Publisher publisher) {
        this.publisher = publisher;
        return this;
    }
    
    public Server setSubscriber(Subscriber subscriber) {
        this.subscriber = subscriber;
        return this;
    }
    
    private static Log log = LogFactory.getLog(Server.class);
    
    private Subscriber subscriber;
    private Publisher publisher;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private InternetAddress localAddress;
    private Thread thread = null;   

}
