package databus.util;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Recorder {
    public Recorder(String fileName) throws IOException {
        properties = new Properties();
        File file = new File(fileName);
        if (file.isFile() && file.exists()) {
            properties.load(Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8));
        }
        
        fileChannel = FileChannel.open(file.toPath(),
                                         StandardOpenOption.CREATE,
                                         StandardOpenOption.TRUNCATE_EXISTING,
                                         StandardOpenOption.WRITE);   
        outputStream = new WrapperByteArrayOutputStream();
        osPrevSize = 0;
        writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
    }
    
    public Properties loadProperties() {
        return new Properties(properties);
    }
    
    public synchronized void saveProperties(String...recordedPairs) {
        int len = recordedPairs.length;
        if ((len%2) != 0) {
            log.error("recoredPairs must be even!");
            return;
        }
        for(int i=0; i<len; i+=2) {
            properties.put(recordedPairs[i], recordedPairs[i+1]);
        }
        try {
            properties.store(writer, null);
            int currentSize = outputStream.size();
            for(int i=currentSize; i<osPrevSize; i++) {
                outputStream.write(' ');
            }
            osPrevSize = currentSize;
            fileChannel.write(outputStream.toByteBuffer(), 0);
            fileChannel.truncate(currentSize);
            outputStream.reset();
        } catch (IOException e) {
            log.error("Can't save : "+properties.toString(), e);
        }        
    }
    
    private static Log log = LogFactory.getLog(Recorder.class);
    
    private FileChannel fileChannel;
    private BufferedWriter writer;
    private WrapperByteArrayOutputStream outputStream;
    private int osPrevSize;
    private Properties properties;
    
    private static class WrapperByteArrayOutputStream extends ByteArrayOutputStream {

        public WrapperByteArrayOutputStream() {
            super();
        }
        
        public ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(buf);
        }
    }
}
