package databus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Recorder {
    public Recorder(String fileName) throws IOException {
        File file = new File(fileName);
        path = file.toPath().normalize();
        if (file.exists()) {
            if (file.isFile()) {
                loadCache();
            } else {
                log.error("Can't read from "+fileName);
                throw new IOException(fileName+" is't a file");
            }           
        } else {
            cache = new HashMap<String, String>();
        }
    }
    
    public synchronized Map<String, String> getData() {
        return new HashMap<String, String>(cache);        
    }
    
    public synchronized  String getDatum(String key) {
        return cache.get(key);
    }
    
    public synchronized void save(Map<String, String> data) {
        cache = new HashMap<String, String>(data);
        saveCache();
    }
    
    public synchronized void save(String...dataPairs) {
        int len = dataPairs.length;
        if ((len%2) != 0) {
            log.error("recoredPairs must be even!");
            return;
        }
        for(int i=0; i<len; i+=2) {
            cache.put(dataPairs[i], dataPairs[i+1]);
        } 
        saveCache();
    }
    
    private void loadCache() throws IOException {
        HashMap<String, String> data = new HashMap<String, String>(); 
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);) {
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
        }
        
        cache = data;
    }

    private void saveCache() { 
        try(BufferedWriter writer = Files.newBufferedWriter(
                                            path, 
                                            StandardCharsets.UTF_8,
                                            StandardOpenOption.CREATE,
                                            StandardOpenOption.TRUNCATE_EXISTING,
                                            StandardOpenOption.WRITE);
                                          ) {      
            writer.write("#" + new Date().toString());            
            for(Map.Entry<String, String> e : cache.entrySet()) {
                writer.newLine();
                writer.append(e.getKey())
                      .append('=')
                      .append(e.getValue());
            }
            writer.flush();
        } catch (IOException e) {
            log.error("Can't write "+cache.toString()+" to "+path.getFileName());
        }
        
    }
    
    private static Log log = LogFactory.getLog(Recorder.class);
    
    private Path path;
    private HashMap<String, String> cache;
}
