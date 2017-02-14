package com.dp.blackhole.agent;

import java.util.concurrent.atomic.AtomicReference;

public class LogFSM {
    public enum LogState {NEW, APPEND, HALT, FINISHED}
    private AtomicReference<LogState> currentLogState = new AtomicReference<LogState>(LogState.NEW);
    
    public LogState getCurrentLogStatus() {
        return currentLogState.get();
    }
    
    public void resetCurrentLogStatus() {
        currentLogState.set(LogState.NEW);
    }

    public void doFileAppend() {
        currentLogState.compareAndSet(LogState.APPEND, LogState.APPEND);
    }
    
    public void doFileAppendForce() {
        currentLogState.set(LogState.APPEND);
    }
    
    public void beginHalt() {
        currentLogState.set(LogState.HALT);
    }
    
    public void finishHalt() {
        currentLogState.compareAndSet(LogState.HALT, LogState.FINISHED);
    }
}
