package databus.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;

public class ClientHandler  extends ChannelInboundHandlerAdapter{
    
    public ClientHandler(String message) {
        super();
        this.message = message;
    } 

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.writeAndFlush(Unpooled.copiedBuffer(message, CharsetUtil.UTF_8));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                                                             throws Exception {
       String address =  ctx.channel().remoteAddress().toString();
       log.error(message+" cannot send to "+address, cause);
    }
    
    private static Log log = LogFactory.getLog(ClientHandler.class);
    
    private String message;
}
