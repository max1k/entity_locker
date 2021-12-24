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

/**
 * Reentrant implementation of {@link EntityLocker}.
 * Allows take nested locks on a same entity and prevent deadlocks by throwing {@link DeadLockPreventException}
 * in case of taking nested locks on different entities.
 * @param <T> Type of entities that EntityLocker works with.
 */
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
        runExclusive(entityId, runnable, null);
    }

    @Override
    public boolean lockAndRun(@NotNull T entityId, @NotNull Runnable runnable, @NotNull Duration duration) {
        return runExclusive(entityId, runnable, duration);
    }

    private boolean runExclusive(@NotNull T entityId, @NotNull Runnable runnable, @Nullable Duration duration) {
        try {
            pushEntityToThreadStack(entityId);
            return runWithCleanupWrapper(entityId, runnable, duration);
        } finally {
            if (!cleanUpThreadEntitiesAndGetReentrantMode()) {
                cleanupEntityLock(entityId);
            }
        }
    }

    private boolean runWithCleanupWrapper(T entityId, Runnable runnable, Duration duration) {
        final Lock cleanupReadLock = getCleanUpLock(entityId).readLock();

        cleanupReadLock.lock();
        try {
            return runWithEntityLockWrapper(entityId, runnable, duration);
        } catch (InterruptedException e) {
            throw new InterruptedEntityLockRuntimeException(e);
        } finally {
            cleanupReadLock.unlock();
        }
    }

    private boolean runWithEntityLockWrapper(T entityId, Runnable runnable, Duration duration) throws InterruptedException {
        final Lock entityLock = getEntityLock(entityId);

        if (duration == null) {
            entityLock.lock();
        } else {
            if (!entityLock.tryLock(duration.toNanos(), TimeUnit.NANOSECONDS)) {
                return false;
            }
        }

        try {
            runnable.run();
            return true;
        } finally {
            entityLock.unlock();
        }
    }

    private Lock getEntityLock(T entityId) {
        return lockByEntityID.computeIfAbsent(entityId, t -> new ReentrantLock());
    }

    private ReadWriteLock getCleanUpLock(T entityId) {
        final int lockIndex = entityId.hashCode() & (CLEANUP_LOCKS_SIZE - 1);
        return cleanUpLocks.get(lockIndex);
    }

    private void cleanupEntityLock(T entityId) {
        final Lock cleanUpWriteLock = getCleanUpLock(entityId).writeLock();

        cleanUpWriteLock.lock();
        try {
            lockByEntityID.remove(entityId);
        } finally {
            cleanUpWriteLock.unlock();
        }
    }

    private boolean cleanUpThreadEntitiesAndGetReentrantMode() {
        final Stack<T> threadEntities = getCurrentThreadLockedEntities();
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
            throw new DeadLockPreventException("Trying to take nested non-reentrant lock: " + threadEntities);
        }
    }

    private Stack<T> getCurrentThreadLockedEntities() {
        return lockedEntitiesByThread.get(Thread.currentThread());
    }

    /**
     * Package access. For test cases only
     * @return true if instance has empty supporting Maps
     */
    boolean isClean() {
        return lockByEntityID.isEmpty() && lockedEntitiesByThread.isEmpty();
    }
}
