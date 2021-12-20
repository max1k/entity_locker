package ru.mxk.util;


import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

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
    private static final int CLEANUP_LOCKS_SIZE = 32;

    /**
     * Entity and support locks
     */
    private final Map<T, Lock> lockByEntityID = new ConcurrentHashMap<>();
    private final Map<Thread, Stack<T>> lockedEntitiesByThread = new ConcurrentHashMap<>();
    private final List<ReadWriteLock> cleanUpLocks = Stream.generate(ReentrantReadWriteLock::new)
                                                           .limit(CLEANUP_LOCKS_SIZE)
                                                           .collect(Collectors.toList());

    @Override
    public void lockAndRun(@NotNull T entityId, @NotNull Runnable runnable) {
        runExclusiveUnderCleanUpLock(entityId, runnable, null);
    }

    @Override
    public boolean lockAndRun(@NotNull T entityId, @NotNull Runnable runnable, @NotNull Duration duration) {
        return runExclusiveUnderCleanUpLock(entityId, runnable, duration);
    }

    private boolean runExclusiveUnderCleanUpLock(@NotNull T entityId, @NotNull Runnable runnable, @Nullable Duration duration) {
        final Lock cleanUpReadLock = getCleanUpLock(entityId).readLock();
        boolean locked = false;

        try {
            cleanUpReadLock.lock();
            final Lock entityLock = getEntityLock(entityId);

            try {
                if (duration == null) {
                    entityLock.lock();
                    locked = true;
                } else {
                    locked = entityLock.tryLock(duration.toNanos(), TimeUnit.NANOSECONDS);
                }

                if (locked) {
                    runnable.run();
                }
            } catch (InterruptedException e) {
                throw new InterruptedEntityLockRuntimeException();
            } finally {
                if (locked) {
                    entityLock.unlock();
                }
            }
        } finally {
            cleanUpReadLock.unlock();
        }

        final boolean reentrantMode = cleanUpThreadEntitiesAndGetReentrantMode();
        if (locked && !reentrantMode) {
            cleanUp(entityId);
        }

        return locked;
    }

    private Lock getEntityLock(T entityId) {
        pushEntityToThreadStack(entityId);
        return lockByEntityID.computeIfAbsent(entityId, t -> new ReentrantLock());
    }

    private ReadWriteLock getCleanUpLock(T entityId) {
        final int lockIndex = entityId.hashCode() & (CLEANUP_LOCKS_SIZE - 1);
        return cleanUpLocks.get(lockIndex);
    }

    private void cleanUp(T entityId) {
        final Lock cleanUpWriteLock = getCleanUpLock(entityId).writeLock();
        try {
            cleanUpWriteLock.lock();
            lockByEntityID.remove(entityId);
        } finally {
            cleanUpWriteLock.unlock();
        }
    }

    private boolean cleanUpThreadEntitiesAndGetReentrantMode() {
        final Stack<T> threadEntities = lockedEntitiesByThread.get(Thread.currentThread());
        threadEntities.pop();

        if (threadEntities.isEmpty()) {
            lockedEntitiesByThread.remove(Thread.currentThread());
            return false;
        }

        return true;
    }

    private void pushEntityToThreadStack(T entityId) {
        final Stack<T> threadEntities =
                lockedEntitiesByThread.computeIfAbsent(Thread.currentThread(), thread -> new Stack<>());
        final boolean isNotFirstEntity = !threadEntities.isEmpty();
        final T previousEntityId = isNotFirstEntity ? threadEntities.peek() : null;

        threadEntities.push(entityId);

        if (isNotFirstEntity && !Objects.equals(previousEntityId, entityId)) {
            throw new DeadLockPreventException("Trying to take nested non-reentrant lock: " + threadEntities, threadEntities);
        }
    }
}
