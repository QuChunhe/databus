package databus.network;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import databus.core.Event;
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
        buffer = Unpooled.buffer(1 << 10);
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
        log.info(buffer.toString());
        log.info("**********************************************************************8");             
        log.info(ctx.channel().localAddress().toString());
        
        int index = buffer.readerIndex();
        int pos = index;
        int end = index + buffer.readableBytes();
        int start = -1;
        do { 
            start = pos;
            pos = indexOfSplitter(buffer, start, end);
            if (pos >= 0) {
                String message = buffer.toString(start, pos-start, CharsetUtil.UTF_8);
                pos = pos + 12;
                receive0(message, address);
                log.info(message);
            }
        } while (pos >= 0);
        
        buffer.readerIndex(start);
        
       
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, 
                                Throwable cause) throws Exception {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        String localAddress = ctx.channel().localAddress().toString();
        log.error("Connection ("+localAddress+", "+remoteAddress+") has error", cause);
        ctx.close();
    }
    
    private int indexOfSplitter(ByteBuf buf, int start, int end) {        
        int pos = start;
        while (pos >= 0) {
            pos = buf.indexOf(pos, end, (byte)'\r');
            if (pos>0) {
                if (((pos+12)<=end) &&
                         (buf.getByte(pos+1)=='\n') && 
                         (buf.getByte(pos+2)=='\r') &&
                         (buf.getByte(pos+3)=='\n') && 
                         (buf.getByte(pos+4)=='<') && 
                         (buf.getByte(pos+5)=='-') && 
                         (buf.getByte(pos+6)=='-') && 
                         (buf.getByte(pos+7)=='>') && 
                         (buf.getByte(pos+8)=='\r') &&
                         (buf.getByte(pos+9)=='\n') &&
                         (buf.getByte(pos+10)=='\r') &&
                         (buf.getByte(pos+11)=='\n') ) {
                     return pos;
                 } else {
                     pos++;
                 }
            }              
        }
        
        return -1;
    }
    
    private void receive0(String message, InetSocketAddress address) {
        if ((null==message) || (message.length()==0)) {
            return;
        }
        Event event = parser.parse(message);
        if (null == event) {
            log.error("Message from " + address +
                      " cannot be parsed as Event : " + message);
            return;
        }
        
        String ipAddress = address.getAddress().getHostAddress();
        event.ipAddress(ipAddress);

        try {
            if (null != subscriber) {
                subscriber.receive(event);
            }
        } catch (Exception e) {
            log.error(subscriber.getClass().getName()+" can't receive event :" 
                      + event.toString(), e);           
        }
        try {
            if (null != publisher) {
                publisher.receive(event);
            }
        } catch (Exception e) {
            log.error(publisher.getClass().getName()+" can't receive event :" 
                      + event.toString(), e);            
        } 
    }
        
    private static Log log = LogFactory.getLog(ServerHandler.class);
    private static MessageParser parser = new MessageParser();
    
    private Publisher publisher;
    private Subscriber subscriber;
    private ByteBuf buffer;
}
