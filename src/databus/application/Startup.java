package databus.application;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Startup {
    
    public static String getPid() {
        String runtimeBean = ManagementFactory.getRuntimeMXBean().getName();
        if (null == runtimeBean) {
            return null;            
        }

        String[] parts = ManagementFactory.getRuntimeMXBean().getName().split("@");
        if (parts.length < 2) {
            return null;
        }
        
        return parts[0];
    }
    
    public static void savePid(String fileName) {
        String pid = getPid();
        if (null == pid) {
            log.error("Can't get pid");
            return;
        }
        BufferedWriter writer = null;
        
        try {
            writer = Files.newBufferedWriter(Paths.get(fileName), 
                                             StandardCharsets.UTF_8,
                                             StandardOpenOption.CREATE,
                                             StandardOpenOption.TRUNCATE_EXISTING,
                                             StandardOpenOption.WRITE);
            writer.write(pid);
            writer.flush();
        } catch (IOException e) {
            log.error("Can't write "+fileName, e);
        } finally {
            if (null != writer) {
                try {
                    writer.close();
                } catch (IOException e) {
                    log.error("Can't close "+fileName, e);
                }
            }
        }
    }
    
    private static Log log = LogFactory.getLog(Startup.class);

}
