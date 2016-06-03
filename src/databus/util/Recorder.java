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
    
    public Recorder(String fileName){
        File file = new File(fileName);       
        if (file.exists() && !file.isFile()) {        
            log.error(fileName + " isn't a file");
        }
        path = file.toPath().normalize();
    } 
    
    public Map<String, String> load(){
        HashMap<String, String> data = new HashMap<String, String>();
        File file = path.toFile();
        if (!file.exists() || !file.isFile()) {        
            return data;
        }
        
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
        } catch (IOException e) {
            log.error("Can't load data from "+path.getFileName(), e);
        }
        
        return data;
    }

    public void save(Map<String, String> data) {
        if (data.size() == 0) {
            return;
        }
        try(BufferedWriter writer = Files.newBufferedWriter(
                                            path, 
                                            StandardCharsets.UTF_8,
                                            StandardOpenOption.CREATE,
                                            StandardOpenOption.TRUNCATE_EXISTING,
                                            StandardOpenOption.WRITE);
                                          ) {      
            writer.write("#" + new Date().toString());            
            for(Map.Entry<String, String> e : data.entrySet()) {
                writer.newLine();
                writer.append(e.getKey())
                      .append('=')
                      .append(e.getValue());
            }
            writer.flush();
        } catch (IOException e) {
            log.error("Can't save "+data.toString()+" to "+path.getFileName(), e);
        }        
    }

    private static Log log = LogFactory.getLog(Recorder.class);
    
    private Path path = null;
}
