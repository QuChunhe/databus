package databus.network.netty;

import static databus.network.netty.NettyConstants.CHANNEL_IDLE_DURATION_SECONDS;
import static databus.network.netty.NettyConstants.DEFAULT_ZIP;
import static databus.network.netty.NettyConstants.DELIMITER_BUFFER;
import static databus.network.netty.NettyConstants.MAX_FRAME_LENGTH;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Properties;
import java.util.Set;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.core.Receiver;
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

    public void withdraw(String topic, Receiver receiver) {
        Set<Receiver> receiversSet = receiversMap.get(topic);
        if (null == receiversSet) {
            log.error(
                    "Don't contain the RemoteTopic " + topic.toString());
        } else {
            if (!receiversSet.remove(receiver)) {
                log.error("Don't contain the receiver " + receiver.toString());
            } else if (receiversSet.size() == 0) {
                remove(topic);
            }
        }
    }
    
    @Override
    public boolean receive(Event event) {
        boolean hasReceived = receive0(event.topic(), event);
        if (event.fullTopic() != null) {
            hasReceived = hasReceived || receive0(event.fullTopic(), event);
        }
         return hasReceived;
    }

    @Override
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
            
            Channel channel = bootstrap.bind().sync().channel();
            channel.closeFuture().sync();
        } catch (Exception e) {
            log.error("Server Thread is interrupted", e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
        
    }
  
    protected void remove(String topic) {
        Set<Receiver> receivers = receiversMap.get(topic);
        if (null != receivers) {
            receivers.clear();
        }
        receiversMap.remove(topic);
    }

    private static Log log = LogFactory.getLog(NettySubscriber.class);
    
    private int workerPoolSize = 1;
    
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private SocketAddress localAddress;
    private StringEncoder stringEncoder = new StringEncoder(CharsetUtil.UTF_8);
    private StringDecoder stringDecoder = new StringDecoder(CharsetUtil.UTF_8);

}
