package com.awesome.flink.internal.concurrent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TableLock {
    private final ReadWriteLock rwLock;
    private final Lock readLock;
    private final Lock writeLock;
    private final Lock versionLock;

    public TableLock() {
        this.rwLock = new ReentrantReadWriteLock();
        this.readLock = rwLock.readLock();
        this.writeLock = rwLock.writeLock();
        this.versionLock = new ReentrantLock();
    }

    public Lock getReadLock() {
        return readLock;
    }

    public Lock getWriteLock() {
        return writeLock;
    }

    public Lock getVersionLock() {
        return versionLock;
    }
}
