package databus.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.util.CharsetUtil;

public final class NetConstants {
    
    public static ZlibWrapper DEFAULT_ZIP = ZlibWrapper.GZIP;
    
    public static String DELIMITER_STRING = "\r\n\r\n<-->\r\n\r\n";
    
    public static ByteBuf DELIMITER_BUFFER = Unpooled.copiedBuffer(DELIMITER_STRING, 
                                                                         CharsetUtil.UTF_8);
    
    public static int CHANNEL_IDLE_DURATION_SECONDS = 10;
    
    public static int MAX_FRAME_LENGTH = 102400;    
    
    public static int CONNECTING_LISTENER_LIMIT_PER_THREAD = 512;
            
    public static int TASK_CAPACITY = 1024;
    
    public static int MAX_CONNECTION_PER_THREAD = 10;
    
}
