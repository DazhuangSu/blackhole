package com.dp.blackhole.agent;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;

import net.contentobjects.jnotify.IJNotify;
import net.contentobjects.jnotify.JNotifyException;
import net.contentobjects.jnotify.JNotifyListener;

public class FileListener implements JNotifyListener{
    private static final Log LOG = LogFactory.getLog(FileListener.class);
    public static final int FILE_CREATED    = 0x1;
    public static final int FILE_DELETED    = 0x2;
    public static final int FILE_MODIFIED   = 0x4;
    public static final int FILE_RENAMED    = 0x8;
    public static final int FILE_ANY        = FILE_CREATED | FILE_DELETED | FILE_MODIFIED | FILE_RENAMED;
    private IJNotify iJNotifyInstance;
    private ConcurrentHashMap<String, RotateQueue> rotateQueueMap;
    private CopyOnWriteArraySet<String> parentWathchPathSet;
    private ConcurrentHashMap<String, Integer> path2wd;
    // Lock the path2wd map only to avoid non-atomic operation
    private Lock lock = new ReentrantLock();

    public FileListener() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        iJNotifyInstance = (IJNotify) Class.forName("net.contentobjects.jnotify.linux.JNotifyAdapterLinux").newInstance();
        rotateQueueMap = new ConcurrentHashMap<String, RotateQueue>();
        //guarantee by synchronized
        parentWathchPathSet = new CopyOnWriteArraySet<String>();
        path2wd = new ConcurrentHashMap<String, Integer>();
    }

    public boolean registerLogReader(final String watchPath, final RotateQueue rotateQueue) {
        int fwd;
        boolean re = false;
        if (rotateQueueMap.putIfAbsent(watchPath, rotateQueue) == null) {
            String parentPath = Util.getParentAbsolutePath(watchPath);
            if (!parentWathchPathSet.contains(parentPath)) {
                parentWathchPathSet.add(parentPath);
                lock.lock();
                try {
                    fwd = iJNotifyInstance.addWatch(parentPath, FILE_CREATED, false, this);
                    path2wd.put(parentPath, fwd);
                } catch (JNotifyException e) {
                    LOG.error("Failed to add watch for " + parentPath, e);
                    rotateQueueMap.remove(watchPath);
                    parentWathchPathSet.remove(parentPath);
                    return re;
                } finally {
                    lock.unlock();
                }
                LOG.info("Registerring and monitoring parent path " + parentPath + " \"FILE_CREATE\"");
            } else {
                LOG.info("Watch parent path " + parentPath + " has already exist in the Set");
            }
            lock.lock();
            try {
              re = addFile(watchPath, rotateQueue);
            } finally {
              lock.unlock();
            }
            LOG.info("Registerring and monitoring tail file " + watchPath + " \"FILE_MODIFIED\"");
        } else {
            LOG.info("Watch path " + watchPath + " has already exist in the Map");
        }
        return re;
    }
    
    public void unregisterLogReader(final String watchPath, final RotateQueue rotateQueue) {
        lock.lock();
        try {
            rotateQueueMap.remove(watchPath);
        } finally {
            lock.unlock();
        }
        //TODO determine whether listener should remove the watch of parent path 
    }

    private boolean addFile(String watchPath, RotateQueue rotateQueue) {
        boolean watched = false;
        Path path = Paths.get(watchPath);
        while (!watched) {
            RandomAccessFile raf = null;
            try {
                BasicFileAttributes faBeforeOpenFile = Files.readAttributes(path, BasicFileAttributes.class);
                raf = new RandomAccessFile(watchPath, "r");
                BasicFileAttributes faAfterOpenFile = Files.readAttributes(path, BasicFileAttributes.class);
                if (faBeforeOpenFile.fileKey().equals(faAfterOpenFile.fileKey())) {
                    watched = true;
                    rotateQueue.handleRotate(raf, faBeforeOpenFile);
                    break;
                }
            } catch (JNotifyException e) {
                LOG.fatal("Failed to add or remove watch for " + watchPath, e);
            } catch (FileNotFoundException e) {
                LOG.error("fileCreated is triggered but fail to get RandomAccessFile, path: " + watchPath);
            } catch (IOException e) {
                LOG.error("Failed to get file attributes for " + watchPath, e);
            } finally {
                if (raf != null && !watched) {
                    try {
                        raf.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            LOG.error("a rotation happened when trying to add watch, filePath: " + watchPath
                    + ", re-watch after 1 sec...");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        return watched;
    }

    /**
     * trigger by file create in parent path
     * if created file is which we watched,
     * remove associated wd from inotify and
     * add watch again (also persistent new wd).
     */
    @Override
    public void fileCreated(int wd, String rootPath, String name) {
        String createdFilePath = rootPath + "/" + name;
        RotateQueue rotateQueue;
        if ((rotateQueue = rotateQueueMap.get(createdFilePath)) != null) {
            LOG.info("rotate detected of " + createdFilePath);
            //Here, we lock to removing the old and adding a new path as a atomic operation.
            lock.lock();
            try {
                //log status modification must happen after putting newWd to path2wd map,
                //if not, it may bring a serious bug
                addFile(createdFilePath, rotateQueue);
                LOG.info("Re-monitoring "+ createdFilePath + " \"FILE_MODIFIED\" for rotate.");
            } finally {
                lock.unlock();
            }
        } else {
            LOG.info("create file " + createdFilePath + " is not in reader map");
        }
    }

    @Override
    public void fileDeleted(int wd, String rootPath, String name) {
    }

    @Override
    public void fileModified(int wd, String rootPath, String name) {
    }

    @Override
    public void fileRenamed(int wd, String rootPath, String oldName,
            String newName) {
    }
}