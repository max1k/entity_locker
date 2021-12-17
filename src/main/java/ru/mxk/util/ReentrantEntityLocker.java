package ru.mxk.util;


import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
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
     * Count of locks for cleanup. Should be a power of two.
     */
    private static final int CLEANUP_LOCKS_SIZE = 16;

    /**
     * Entity and support locks
     */
    private final Map<T, ReentrantLock> lockByEntityID = new ConcurrentHashMap<>();
    private final Map<Thread, Stack<T>> lockedEntitiesByThread = new ConcurrentHashMap<>();
    private final List<ReadWriteLock> cleanUpLocks = Stream.generate(ReentrantReadWriteLock::new)
                                                           .limit(CLEANUP_LOCKS_SIZE)
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
            if (entityLock != null) {
                entityLock.unlock();
            }

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
        pushEntityToThreadStack(entityId);
        return lockByEntityID.computeIfAbsent(entityId, t -> new ReentrantLock());
    }

    private void pushEntityToThreadStack(T entityId) {
        Stack<T> threadEntityStack = lockedEntitiesByThread.computeIfAbsent(Thread.currentThread(), thread -> new Stack<>());

        boolean threadEntityStackIsNotEmpty = !threadEntityStack.isEmpty();
        T previousEntityId = threadEntityStackIsNotEmpty ? threadEntityStack.peek() : null;

        threadEntityStack.push(entityId);

        if (threadEntityStackIsNotEmpty && !Objects.equals(previousEntityId, entityId)) {
            throw new DeadLockPreventException("Trying to take nested non-reentrant lock: " + threadEntityStack, threadEntityStack);
        }
    }

    private ReadWriteLock getCleanUpLock(T entityId) {
        final int lockIndex = entityId.hashCode() & (CLEANUP_LOCKS_SIZE - 1);
        return cleanUpLocks.get(lockIndex);
    }

    private void cleanUp(T entityId, ReentrantLock entityLock) {
        final boolean reentrantMode = cleanUpThreadEntityStackAndGetReentrantMode();
        final boolean entityLockHasWaiters = entityLock != null && entityLock.getQueueLength() > 0;

        if (entityLockHasWaiters || reentrantMode) {
            return;
        }

        final Lock cleanUpWriteLock = getCleanUpLock(entityId).writeLock();
        try {
            cleanUpWriteLock.lock();
            lockByEntityID.remove(entityId);
        } finally {
            cleanUpWriteLock.unlock();
        }
    }

    private boolean cleanUpThreadEntityStackAndGetReentrantMode() {
        Stack<T> threadEntityStack = lockedEntitiesByThread.get(Thread.currentThread());
        threadEntityStack.pop();

        if (threadEntityStack.isEmpty()) {
            lockedEntitiesByThread.remove(Thread.currentThread());
            return false;
        }

        return true;
    }
}
