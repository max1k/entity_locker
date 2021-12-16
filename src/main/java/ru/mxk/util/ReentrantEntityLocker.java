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
     * Count of clear locks should be a power of two
     */
    private static final int CLEAR_LOCKS_SIZE = 16;

    /**
     * Entity and support locks
     */
    private final Map<T, ReentrantLock> lockByEntityID = new ConcurrentHashMap<>();
    private final Map<Thread, Stack<T>> lockedEntitiesByThread = new ConcurrentHashMap<>();
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
        Stack<T> threadEntityStack = lockedEntitiesByThread.computeIfAbsent(Thread.currentThread(), thread -> new Stack<>());

        boolean threadEntityStackIsNotEmpty = !threadEntityStack.isEmpty();
        T previousEntityId = threadEntityStackIsNotEmpty ? threadEntityStack.peek() : null;

        threadEntityStack.push(entityId);

        if (threadEntityStackIsNotEmpty && !Objects.equals(previousEntityId, entityId)) {
            throw new DeadLockPreventException("Trying to take nested non-reentrant lock: " + threadEntityStack, threadEntityStack);
        }

        return lockByEntityID.computeIfAbsent(entityId, t -> new ReentrantLock());
    }

    private ReadWriteLock getCleanUpLock(T entityId) {
        final int lockIndex = entityId.hashCode() & (CLEAR_LOCKS_SIZE - 1);
        return cleanUpLocks.get(lockIndex);
    }

    private void cleanUp(T entityId, ReentrantLock entityLock) {
        final boolean reentrantMode = cleanUpThreadEntities();
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
     * @return true if entity locker is in reentrant mode
     */
    private boolean cleanUpThreadEntities() {
        Stack<T> threadEntityStack = lockedEntitiesByThread.get(Thread.currentThread());
        threadEntityStack.pop();

        if (threadEntityStack.isEmpty()) {
            lockedEntitiesByThread.remove(Thread.currentThread());
            return false;
        }

        return true;
    }
}
