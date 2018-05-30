package databus.task;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by Qu Chunhe on 2017-04-01.
 */
public class TargetDirectory {
    public TargetDirectory(String directory) {
        this.directory = directory;
        lastModifiedTimeMap = new HashMap<>();
    }

    public void addTargetFile(String fileName) {
        lastModifiedTimeMap.put(fileName, getFile(fileName).lastModified());
    }

    public Set<String> getModifiedFiles() {
        HashSet<String> modifiedSet = new HashSet<>();
        for (Map.Entry<String, Long> entry : lastModifiedTimeMap.entrySet()) {
            File file = getFile(entry.getKey());
            long lastModifiedTime = file.lastModified();
            if (lastModifiedTime > entry.getValue()) {
                try {
                    log.info( entry.getValue()+"-->"+lastModifiedTime);
                    modifiedSet.add(file.getCanonicalPath());
                    entry.setValue(lastModifiedTime);
                } catch (IOException e) {
                    log.error("Can not get modified time for "+entry.getValue(), e);
                }

            }
        }
        return modifiedSet;
    }

    private File getFile(String fileName) {
        return new File(directory, fileName);
    }

    private final static Log log = LogFactory.getLog(TargetDirectory.class);

    private final String directory;
    private final Map<String, Long> lastModifiedTimeMap;
}
