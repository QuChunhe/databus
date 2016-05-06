package databus.network;


import java.net.SocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Startable;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;

import static databus.network.NettyConstants.*;

public class NettyServer implements Startable{    
    
    public NettyServer(SocketAddress localAddress) {
        this(localAddress, 1);
    }
    
    public NettyServer(SocketAddress localAddress, int workerPoolSize) {
        this.localAddress = localAddress;
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(workerPoolSize);
    }
    
    @Override
    public boolean isRunning() {
        return (null!=thread) && (thread.getState()!=Thread.State.TERMINATED);
    }
    
    @Override
    public void start() {
        if (null == thread) {
            thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                run0();               
                            }                
                         }, "Netty Server");
            thread.start();
        }
    }

    public void join() throws InterruptedException {
        thread.join();    
    }
    
    public void stop() {        
        if (null != thread) {
            thread.interrupt();
        }
    }
    
    public NettyServer setSubscriber(NettySubscriber subscriber) {
        this.subscriber = subscriber;
        return this;
    }
    
    private void run0() {        
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.option(ChannelOption.SO_BACKLOG, 1024)
                     .option(ChannelOption.TCP_NODELAY, true)
                     .option(ChannelOption.SO_KEEPALIVE, true)
                     .group(bossGroup, workerGroup)
                     .channel(NioServerSocketChannel.class)
                     .localAddress(localAddress)
                     .childHandler(new ChannelInitializer<SocketChannel>(){
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new IdleStateHandler(0, 0, CHANNEL_IDLE_DURATION_SECONDS))
                             .addLast(new NettyIdleConnectionHandler())
                             .addLast(ZlibCodecFactory.newZlibDecoder(DEFAULT_ZIP))
                             .addLast(new DelimiterBasedFrameDecoder(
                                              MAX_FRAME_LENGTH, DELIMITER_BUFFER
                                          )
                                     )
                             .addLast(stringEncoder)
                             .addLast(stringDecoder)
                             .addLast(new NettyServerHandler(subscriber));
                        }                         
                     });
            
            Channel channel = bootstrap.bind().sync().channel();
            channel.closeFuture().sync();
        } catch (Exception e) {
            log.error("Server Thread is interrupted", e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
    
    private static Log log = LogFactory.getLog(NettyServer.class);
    
    private NettySubscriber subscriber;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private SocketAddress localAddress;
    private Thread thread = null;   
    private StringEncoder stringEncoder = new StringEncoder(CharsetUtil.UTF_8);
    private StringDecoder stringDecoder = new StringDecoder(CharsetUtil.UTF_8);
}
