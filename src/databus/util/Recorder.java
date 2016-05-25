package databus.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Recorder {
    public Recorder(String fileName) throws IOException {
        properties = new Properties();
        this.fileName = fileName;
        File file = new File(fileName);
        if (file.isFile() && file.exists()) {
            properties.load(Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8));
        }
        
        writer = Files.newBufferedWriter(Paths.get(fileName), 
                                         StandardCharsets.UTF_8,
                                         StandardOpenOption.CREATE,
                                         StandardOpenOption.TRUNCATE_EXISTING,
                                         StandardOpenOption.WRITE);        
    }
    
    public Properties loadProperties() {
        return properties;
    }
    
    public void saveProperties(Properties properties) {
        this.properties = properties;
        try {
            properties.store(writer, "time="+System.currentTimeMillis());
        } catch (IOException e) {
            log.error("Can't save to "+fileName+" : "+properties.toString(), e);
        }        
    }
    
    private static Log log = LogFactory.getLog(Recorder.class);
    
    private String fileName;
    private BufferedWriter writer = null;
    private Properties properties;
}
