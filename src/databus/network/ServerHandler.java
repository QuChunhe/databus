package databus.network;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
import databus.util.InternetAddress;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;


public class ServerHandler extends ChannelInboundHandlerAdapter {
    public ServerHandler(Publisher publisher, Subscriber subscriber) {
        super();
        this.publisher = publisher;
        this.subscriber = subscriber;
        buffer = Unpooled.buffer(1024);
        buffer.clear();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
        ByteBuf in;
        if (data instanceof ByteBuf) {
             in = (ByteBuf) data;
        } else {
            log.error(data.getClass().getName()+" cannot cast to ByteBuf");
            return;
        }
        buffer.writeBytes(in);
       
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
        ctx.flush();
        String message = buffer.toString(CharsetUtil.UTF_8);
        log.info("Have received message : " + message);
        try {
            Event event = parser.parse(message);
            if (null == event) {
                log.error("Message from "+address +
                          " cannot be parsed as Event : " + message);
                return;
            }
            String ipAddress = address.getAddress().getHostAddress();
            int port = address.getPort();
            InternetAddress remoteAddress = new InternetAddress(ipAddress, port);
            event.address(remoteAddress);
            if (null != subscriber) {
                subscriber.receive(event);
            }
            if (null != publisher) {
                publisher.receive(event);
            }
        } catch (Exception e) {
            log.error("Can't receive "+ message, e);            
        } finally {
            ctx.close();
        }
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, 
                                Throwable cause) throws Exception {
        String address = ctx.channel().remoteAddress().toString();
        log.error("Cannot read from "+address, cause);
        ctx.close();
    }
        
    private static Log log = LogFactory.getLog(ServerHandler.class);
    private static MessageParser parser = new MessageParser();
    
    private Publisher publisher;
    private Subscriber subscriber;
    private ByteBuf buffer;
}
