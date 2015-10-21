package databus.network;

import java.text.DateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;

/**
 * 
 * SeverHandler is thread-safe.
 *
 */
public class ServerHandler  extends ChannelInboundHandlerAdapter{    

    public ServerHandler() {
        gson = new GsonBuilder().enableComplexMapKeySerialization() 
                                .serializeNulls()   
                                .setDateFormat(DateFormat.LONG)
                                .create();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, 
                            Object msg) throws Exception {
        ByteBuf in;
        if (msg instanceof ByteBuf) {
             in = (ByteBuf) msg;
        } else {
            log.error(msg.getClass().getName()+" cannot cast to ByteBuf");
            return;
        }
        
        String data = in.toString(CharsetUtil.UTF_8);
        System.out.println(data);        
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx)
                                                  throws Exception {
       
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, 
                                Throwable cause) throws Exception {

    }
    
    private static Log log = LogFactory.getLog(ServerHandler.class);
    
    private Gson gson;
}
