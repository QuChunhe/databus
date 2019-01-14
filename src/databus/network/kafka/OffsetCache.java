package databus.network.kafka;

import java.util.concurrent.atomic.AtomicLong;

import databus.util.Backup;

public class OffsetCache {

    public OffsetCache(String file) {
        this(file, -1);
    }
    
    public OffsetCache(String file, int writePerFlush) {
         this.writePerFlush = writePerFlush;
         this.file = file;
         this.counter = new AtomicLong(0);
    }
    
    public long get(String topic, int partition) {
        String value = Backup.instance()
                             .getRecordCache(file)
                             .get(toKey(topic, partition));
        if (null == value) {
            return -1;
        }
        return Long.parseLong(value);
    }
    
    public OffsetCache set(String topic, int partition, long offset) {
        String key = toKey(topic, partition);
        long oldOffset;
        while ((oldOffset=get(topic, partition)) < offset) {
            if ((offset<0) || Backup.instance()
                                    .getRecordCache(file)
                                    .cache(key, Long.toString(oldOffset), Long.toString(offset))) {
                break;
            }
        }
        long c = counter.addAndGet(1);
        if ((writePerFlush>0) && (c%writePerFlush==0)) {
            save();
        }
        return this;
    }
    
    public OffsetCache save() {
        Backup.instance().getRecordCache(file).save();
        return this;
    }

    private String toKey(String topic, int partition) {
        return topic+"||"+partition;
    }

    private final int writePerFlush;
    private final String file;
    private final AtomicLong counter;
}
