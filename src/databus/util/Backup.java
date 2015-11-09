package databus.util;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Backup {
    
    public static Backup instance() {
        return instance;
    }
    
    public void store(String name, Object copy) {
        String fileName = BACKUP_DIR_NAME+"/"+name+".data";
        File bacupFile = new File(fileName);
        if (!bacupFile.exists()) {
            try {
                bacupFile.createNewFile();
            } catch (IOException e) {
                log.error("Can't create file "+fileName);
            }
        }
        FileOutputStream fileStream = null;        
        try {
            fileStream = new FileOutputStream(bacupFile);
            ObjectOutputStream objectStream = new ObjectOutputStream(fileStream);
            objectStream.writeObject(copy);
            objectStream.flush();
            objectStream.close();
        } catch (FileNotFoundException e) {
            log.error(fileName + " does't exist", e);
        } catch(IOException e){
            log.error("Can't write "+fileName,e);
        } finally {
            close(fileStream);
        }
    }
    
    
    @SuppressWarnings("unchecked")
    public <T> T restore(String name, T t) {
        String fileName = BACKUP_DIR_NAME+"/"+name+".data";
        File backupFile = new File(fileName);
        if (!backupFile.exists()) {
            log.info(name+" has't store");
            return null;
        }
        FileInputStream fileStream = null;
        T copy = null;
        try {
            fileStream = new FileInputStream(backupFile);
            ObjectInputStream objectStream = new ObjectInputStream(fileStream);
            Object c = objectStream.readObject();
            if (t.getClass().isInstance(c)) {
                copy = (T) c;
            } else {
                log.error(c.getClass().getName()+" can't cast to "+
                          t.getClass().getName());
            }
            objectStream.close();
        } catch (IOException e) {
            log.error("Can't read " + t.getClass().getSimpleName() + " object" +
                      " from " + fileName);
        } catch(ClassNotFoundException e) {
            log.error("Can't find class " + t.getClass().getSimpleName() +
                      " info in "+fileName);
        } finally {
            close(fileStream);
        }
        
        return copy;
    }

    private void close (Closeable io) {
        try {
            if (null != io) {
               io.close(); 
            }            
        } catch (IOException e) {
            log.error("Can't close "+io.toString(),e);
        }
    }
    
    private static final String BACKUP_DIR_NAME = "data";
    
    private static Log log = LogFactory.getLog(Backup.class);
    private static Backup instance = new Backup();
}
