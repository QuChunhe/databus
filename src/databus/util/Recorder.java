package databus.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Recorder {
    
    public Recorder(String recordFileName){
        this.recordFileName = recordFileName;
    } 
    
    public Map<String, String> load(){
        HashMap<String, String> data = new HashMap<>();
        File file = new File(recordFileName);
        if (!file.exists() || !file.isFile()) {        
            return data;
        }
        
        try (BufferedReader reader = Files.newBufferedReader(file.toPath(), 
                                                             StandardCharsets.UTF_8)) {
            String line;
            while((line=reader.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split("=", 2);
                if (parts.length == 2) {
                    data.put(parts[0].trim(), parts[1].trim());
                }
            }
        } catch (IOException e) {
            log.error("Can't load data from "+recordFileName, e);
        }
        
        return data;
    }

    public void save(Map<String, String> data) {
        if (data.size() == 0) {
            return;
        }
        FileChannel fileChannel = getRecordFileChannel(recordFileName);
        if (null == fileChannel) {
            log.error("FileChannel is null, and can't save "+data.toString());
            return;
        }
        StringBuilder builder = new StringBuilder(128);
        builder.append("#")
               .append(new Date().toString());
        for(Map.Entry<String, String> e : data.entrySet()) {
            builder.append(LINE_SEPERATOR)
                   .append(e.getKey())
                   .append('=')
                   .append(e.getValue());
        }
        synchronized (this) {
            //guarantee atomic overwrite
            long currentFileLength = builder.length();
            for(long i=currentFileLength; i<prevFileLength; i++) {
                builder.append(' ');
            }
            prevFileLength = currentFileLength;
            
            CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
            ByteBuffer byteBuffer;
            try {
                byteBuffer = encoder.encode(CharBuffer.wrap(builder, 0, builder.length()));
            } catch (CharacterCodingException e) {
                log.error("Can't encode "+builder.toString(), e);
                return;
            }
            int size = byteBuffer.limit() - byteBuffer.position();
        
            try {
                int length = fileChannel.write(byteBuffer, 0);
                fileChannel.truncate(length);
                if (length != size) {
                    log.error(size+" bytes , only "+length+" write to file");
                }
            } catch (IOException e) {
                log.error("Can't write "+data.toString()+" to "+recordFileName);
            }
        }       
    }
    
    public void close() {
        if (null != recordFileChannel) {
            synchronized (this) {
                if (null != recordFileChannel) {
                    try {
                        recordFileChannel.close();
                        recordFileChannel = null;
                    } catch (IOException e) {
                        log.error("Can't close "+recordFileName, e);
                    }
                }
            }
        }
    }
    
    private FileChannel openFileChannel(String fileName) {
        File file = new File(fileName);       
        if (file.exists() && !file.isFile()) {        
            log.error(fileName + " isn't a file");
            return null;
        }
        FileChannel fileChannel = null;
        try {
            fileChannel = FileChannel.open(file.toPath(),
                                           StandardOpenOption.CREATE,
                                           StandardOpenOption.TRUNCATE_EXISTING,
                                           StandardOpenOption.WRITE);
        } catch (IOException e) {
            log.error("Can't open FileChannel for "+fileName);
        }
        return fileChannel;
    }
    
    private FileChannel getRecordFileChannel(String fileName) {
        if (null == recordFileChannel) {
            synchronized (this) {
                if (null == recordFileChannel) {
                    recordFileChannel = openFileChannel(fileName);
                }
            }
        }
        return recordFileChannel;
    }
    

    private static Log log = LogFactory.getLog(Recorder.class);
    private static final String LINE_SEPERATOR = System.getProperty("line.separator", "\\n");
    
    private FileChannel recordFileChannel = null;
    private final String recordFileName;
    private long prevFileLength = 0;
}
