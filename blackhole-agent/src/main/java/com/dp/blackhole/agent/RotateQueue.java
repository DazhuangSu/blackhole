package com.dp.blackhole.agent;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.dp.blackhole.common.Util;

public class RotateQueue {

    private TreeMap<Long, FileInfo> files = null;
    private long period;
    private double clockSyncBufPercent = 0.01;
    private ReentrantReadWriteLock lock;
    private Lock rLock;
    private Lock wLock;

    public RotateQueue(long period) {
        this.period = period;
        this.files = new TreeMap<Long, FileInfo>();
        this.lock = new ReentrantReadWriteLock();
        this.rLock = lock.readLock();
        this.wLock = lock.writeLock();
    }

    public boolean isRotated() {
        rLock.lock();
        try {
            if (files.keySet().size() > 1) {
                return true;
            }
            return false;
        } finally {
            rLock.unlock();
        }
    }

    public void handleRotate(RandomAccessFile raf, BasicFileAttributes fa) {
        wLock.lock();
        try {
            if (!fileAdded(fa)) {
                FileInfo fi = new FileInfo(raf, fa);
                files.put(fi.getCreateTime(), fi);
            }
        } finally {
            wLock.unlock();
        }
    }

    public RandomAccessFile finishRotate() {
        wLock.lock();
        FileInfo raf = files.firstEntry().getValue();
        try {
            files.remove(files.firstKey());
            raf.closeFile();
            return files.firstEntry().getValue().file;
        } finally {
            wLock.unlock();
        }
    }

    public RandomAccessFile getCurrentFile() {
        rLock.lock();
        try {
            return files.firstEntry().getValue().file;
        } finally {
            rLock.unlock();
        }
    }

    public long getCurrentPeriod() {
        rLock.lock();
        try {
            return Util.getCurrentRollTsUnderTimeBuf(files.firstEntry().getKey(), period,
                    (long) (period * clockSyncBufPercent));
        } finally {
            rLock.unlock();
        }
    }

    private boolean fileAdded(BasicFileAttributes fa) {
        for (FileInfo fi : files.values()) {
            if (fi.equalFile(fa)) {
                return true;
            }
        }
        return false;
    }

    private class FileInfo {
        private RandomAccessFile file;
        private BasicFileAttributes fa;

        private FileInfo(RandomAccessFile file, BasicFileAttributes fa) {
            this.file = file;
            this.fa = fa;
        }

        private long getCreateTime() {
            return fa.creationTime().toMillis();
        }

        private boolean equalFile(BasicFileAttributes fa) {
            return this.fa.fileKey().equals(fa.fileKey());
        }

        private void closeFile() {
            try {
                this.file.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
