package databus.network.netty;

import static databus.network.netty.NettyConstants.CHANNEL_IDLE_DURATION_SECONDS;
import static databus.network.netty.NettyConstants.DEFAULT_ZIP;
import static databus.network.netty.NettyConstants.DELIMITER_BUFFER;
import static databus.network.netty.NettyConstants.MAX_FRAME_LENGTH;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Runner;
import databus.network.AbstractSubscriber;
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

public class NettySubscriber extends AbstractSubscriber {

    public NettySubscriber() {
        super();
    }
    
    @Override
    public void initialize(Properties properties) {
        String rawHost = properties.getProperty("netty.host");
        String[] parts = rawHost.split(":");
        String addr = parts[0].trim();
        int port = Integer.parseInt(parts[1]);
        localAddress = new InetSocketAddress(addr, port);
        String threadPoolSize = properties.getProperty("netty.threadPoolSize");
        if (null != threadPoolSize) {
            workerPoolSize = Integer.parseInt(threadPoolSize);
        }      
    }
    
    @Override
    public boolean receive(Event event) {
        boolean hasReceived = receive(event.topic(), event);
        if (event.fullTopic() != null) {
            hasReceived = hasReceived || receive(event.fullTopic(), event);
        }
         return hasReceived;
    }


    protected void run0() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(workerPoolSize);
        NettySubscriber subscriber = this;
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
            
            listeningChannel = bootstrap.bind().sync().channel();
            listeningChannel.closeFuture().sync();
        } catch (Exception e) {
            log.error("Server Thread is interrupted", e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            
        }
        
    } 
    
    @Override
    protected Runner createBackgroundRunner() {
        return new ListeningRunner();
    } 
    
    @Override
    public void stop() {
        super.stop();
        try {
            log.info("Waiting all channels close");
            listeningChannel.close().await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            
        }
    }


    private static Log log = LogFactory.getLog(NettySubscriber.class);
    
    private int workerPoolSize = 1;
    
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private SocketAddress localAddress;
    private StringEncoder stringEncoder = new StringEncoder(CharsetUtil.UTF_8);
    private StringDecoder stringDecoder = new StringDecoder(CharsetUtil.UTF_8);
    private Channel listeningChannel;
    
    private class ListeningRunner implements Runner {

        @Override
        public void initialize() {
            
        }

        @Override
        public void runOnce() {
            run0();            
        }

        @Override
        public void processException(Exception e) {
            
        }

        @Override
        public void stop(Thread owner) {
            
        }

        @Override
        public void close() {
            
        }
        
    }


}
