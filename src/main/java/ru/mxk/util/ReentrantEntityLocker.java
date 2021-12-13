package ru.mxk.util;


import java.time.Duration;
import java.util.LinkedList;
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
     * Entity and support locks
     */
    private final Map<T, ReentrantLock> lockByEntityID = new ConcurrentHashMap<>();
    private final Map<Thread, LinkedList<T>> lockedEntitiesByThread = new ConcurrentHashMap<>();
    private final List<ReadWriteLock> cleanUpLocks = Stream.generate(ReentrantReadWriteLock::new)
                                                           .limit(CLEAR_LOCKS_SIZE)
                                                           .collect(Collectors.toList());

    @Override
    public void lockAndRun(T entityId, Runnable runnable) {
        ReentrantLock entityLock = null;
        final Lock cleanUpReadLock = getCleanUpLock(entityId).readLock();

        try {
            cleanUpReadLock.lock();
            entityLock = getEntityLock(entityId);
            entityLock.lock();
            runnable.run();
        } finally {
            Optional.ofNullable(entityLock).ifPresent(Lock::unlock);

            cleanUpReadLock.unlock();
            cleanUp(entityId, entityLock);
        }
    }

    @Override
    public boolean lockAndRun(T entityId, Runnable runnable, Duration duration) throws InterruptedException {
        ReentrantLock entityLock = null;
        boolean locked = false;
        final Lock cleanUpReadLock = getCleanUpLock(entityId).readLock();

        try {
            cleanUpReadLock.lock();
            entityLock = getEntityLock(entityId);
            if (entityLock.tryLock(duration.toNanos(), TimeUnit.NANOSECONDS)) {
                locked = true;
                runnable.run();
            }
        } finally {
            if (locked) {
                entityLock.unlock();
            }

            cleanUpReadLock.unlock();
            cleanUp(entityId, entityLock);
        }

        return locked;
    }

    private ReentrantLock getEntityLock(T entityId) {
        LinkedList<T> ts = lockedEntitiesByThread.computeIfAbsent(Thread.currentThread(), thread -> new LinkedList<>());
        ts.addLast(entityId);

        if (ts.size() > 1 && ts.get(ts.size() - 2) != entityId) {
            throw new DeadLockPreventException("Trying to take nested non-reentrant lock: " + ts, ts);
        }

        return lockByEntityID.computeIfAbsent(entityId, t -> new ReentrantLock());
    }

    private ReadWriteLock getCleanUpLock(T entityId) {
        final int lockIndex = entityId.hashCode() & (CLEAR_LOCKS_SIZE - 1);
        return cleanUpLocks.get(lockIndex);
    }

    private void cleanUp(T entityId, ReentrantLock entityLock) {
        final boolean reentrantMode = cleanUpThreadEntities(entityId);
        final boolean hasWaiters = entityLock != null && entityLock.getQueueLength() > 0;

        if (hasWaiters || reentrantMode) {
            return;
        }

        final Lock writeLock = getCleanUpLock(entityId).writeLock();
        try {
            writeLock.lock();
            lockByEntityID.remove(entityId);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * @param entityId entityId for cleanUp thread sequence
     * @return true if entity locker is in reentrant mode
     */
    private boolean cleanUpThreadEntities(T entityId) {
        LinkedList<T> threadLockedEntities = lockedEntitiesByThread.get(Thread.currentThread());
        threadLockedEntities.removeLastOccurrence(entityId);

        if (threadLockedEntities.isEmpty()) {
            lockedEntitiesByThread.remove(Thread.currentThread());
            return false;
        }

        return true;
    }
}
