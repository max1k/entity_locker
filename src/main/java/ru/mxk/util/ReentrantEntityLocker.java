package ru.mxk.util;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReentrantEntityLocker<T> implements EntityLocker<T> {
    /**
     * Count of clear locks should be a power of two
     */
    private static final int CLEAR_LOCKS_SIZE = 16;
    /**
     * Waiting for a clear lock timeout
     */
    private static final long CLEAR_WAIT_TIMEOUT_MS = 50;
    private final Map<T, ReentrantLock> lockByEntityID = new ConcurrentHashMap<>();
    private final List<ReadWriteLock> clearLocks = Stream.generate(ReentrantReadWriteLock::new)
                                                         .limit(CLEAR_LOCKS_SIZE)
                                                         .collect(Collectors.toList());

    @Override
    public void lockAndRun(T entityId, Runnable runnable) {
        ReentrantLock entityLock = null;
        final Lock clearReadLock = getClearLock(entityId).readLock();
        try {
            clearReadLock.lock();
            entityLock = getLock(entityId);
            entityLock.lock();

            runnable.run();
        } finally {
            Optional.ofNullable(entityLock).ifPresent(Lock::unlock);
            clearReadLock.unlock();
        }

        clearEntityMap(entityId, entityLock);
    }

    @Override
    public boolean lockAndRun(T entityId, Runnable runnable, Duration duration) throws InterruptedException {
        ReentrantLock entityLock = null;
        boolean result = false;
        final Lock clearReadLock = getClearLock(entityId).readLock();

        try {
            clearReadLock.lock();
            entityLock = getLock(entityId);
            if (entityLock.tryLock(duration.toNanos(), TimeUnit.NANOSECONDS)) {
                result = true;
                runnable.run();
            }

        } finally {
            if (result) {
                entityLock.unlock();
            }
            clearReadLock.unlock();
        }

        clearEntityMap(entityId, entityLock);
        return result;
    }

    private ReentrantLock getLock(T entityId) {
        return lockByEntityID.computeIfAbsent(entityId, t -> new ReentrantLock());
    }

    private void clearEntityMap(T entityId, ReentrantLock entityLock) {
        if (entityLock.getQueueLength() > 0) {
            return;
        }

        final Lock writeLock = getClearLock(entityId).writeLock();
        boolean hasLock = false;

        try {
            if (writeLock.tryLock(CLEAR_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                hasLock = true;
                lockByEntityID.remove(entityId);
            }
        } catch (InterruptedException e) {
            throw new IllegalMonitorStateException();
        } finally {
            if (hasLock) {
                writeLock.unlock();
            }
        }
    }

    private ReadWriteLock getClearLock(T entityId) {
        final int lockIndex = entityId.hashCode() & (CLEAR_LOCKS_SIZE - 1);
        return clearLocks.get(lockIndex);
    }
}
