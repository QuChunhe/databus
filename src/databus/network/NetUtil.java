package databus.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

public final class NetUtil {    
    
    public NetUtil() {
        super();
    }

    public ByteBuf compress(String message) {
        byte[] data = message.getBytes(UTF_8);
        ByteBuf buffer = Unpooled.buffer(1024);
        buffer.writeBytes(START_SEPERATORS);
        try (ByteBufOutputStream os = new ByteBufOutputStream(buffer);
             GZIPOutputStream gos = new GZIPOutputStream(os);) {            
            gos.write(data);
            gos.finish();
            gos.flush();
            os.flush();
        } catch (IOException e) {
            log.error("Cann't be compressed : "+message, e);
        }

        buffer.writeBytes(END_SEPERATORS);
        return buffer;
    }
    
    public String decompress(ByteBuf data) {
        int start = indexOf(data, START_SEPERATORS);
        if (start < 0) {
            return null;
        }
        int end = indexOf(data, END_SEPERATORS);
        if (end < 0) {
            return null;
        }
        start =  start + START_SEPERATORS.length;
        if (start > end) {
            return null;
        }
        
        ByteBuf dulicatedData = data.duplicate();
        dulicatedData.readerIndex(start);
        dulicatedData.writerIndex(end);
        data.readerIndex(end+END_SEPERATORS.length);
        
        ByteBuffer buffer = ByteBuffer.allocate(SIZE);
        buffer.clear();        
        try (ByteBufInputStream is = new ByteBufInputStream(dulicatedData);
             GZIPInputStream gis = new GZIPInputStream(is);) {
            byte[] bytes = new byte[SIZE];
            int count;
            while((count=gis.read(bytes)) != -1) {
                buffer.put(bytes, 0, count);
            }
        } catch (IOException e) {
            log.error("Cann't be decompressed : "+dulicatedData.toString(), e);
        }

        return new String(buffer.array(),0 ,buffer.position(), UTF_8);
    }
    
    public int indexOf(ByteBuf buffer, byte[] find) {
        int index = -1;
        int start = buffer.readerIndex();
        int end = start + buffer.readableBytes();        
        int len = find.length;
        while (start >= 0) {
            start = buffer.indexOf(start, end, find[0]);
            if ((start >= 0) && (end >= (start+len))) {
                boolean match = true;
                for(int i=1; (i<len) && match ; i++) {
                    if (buffer.getByte(start+i) != find[i]) {
                        match = false;
                    }
                }
                if (match) {
                    index = start;
                    break;
                } 
                start++;                
            } else {
                break;
            }
        }
        
        return index;
    }    
    
    public final byte[] START_SEPERATORS = {'\r','\n','\r','\n','<','-'};
    public final byte[] END_SEPERATORS = {'-','>','\r','\n','\r','\n'};
    
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final int SIZE = 1024;
    
    private static Log log = LogFactory.getLog(NetUtil.class);
}
