package databus.network.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.util.CharsetUtil;

public final class NettyConstants {
    
    public final static ZlibWrapper DEFAULT_ZIP = ZlibWrapper.GZIP;
    
    public final static String DELIMITER_STRING = "\r\n\r\n<-->\r\n\r\n";
    
    public final static ByteBuf DELIMITER_BUFFER = Unpooled.copiedBuffer(DELIMITER_STRING, 
                                                                         CharsetUtil.UTF_8);
    
    public final static int CHANNEL_IDLE_DURATION_SECONDS = 1000;
    
    public final static int MAX_FRAME_LENGTH = 102400;    
    
    public final static int DEFAULT_CONNECTING_LISTENERS_PER_THREAD = 512;
    
    public final static int DEFAULT_CONNECTIONS_PER_THREAD = 10;
    
}
