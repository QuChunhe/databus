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
        this.localAddress = localAddress;
        childHandler = new ServerHandler();
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
                        public void initChannel(SocketChannel ch)
                                                         throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(childHandler);
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
        childHandler.setPublisher(publisher);
        return this;
    }
    
    public Server setSubscriber(Subscriber subscriber) {
        childHandler.setSubscriber(subscriber);
        return this;
    }
    
    private static Log log = LogFactory.getLog(Server.class);
    
    private ServerHandler childHandler;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private InternetAddress localAddress;
    private Thread thread = null;   

}
