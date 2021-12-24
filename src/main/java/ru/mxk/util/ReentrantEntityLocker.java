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
        boolean entityLocked = false,
                cleanupLocked = false,
                deadlockOccurred = false;

        try {
            pushEntityToThreadStack(entityId);
            getCleanUpLock(entityId).readLock().lock();
            cleanupLocked = true;

            entityLocked = runExclusive(entityId, runnable, duration);
        } catch (DeadLockPreventException deadLockPreventException) {
            if (deadLockPreventException.getLocker() == this && getCurrentThreadLockedEntities().size() == 1) {
                deadlockOccurred = true;
            }
            throw deadLockPreventException;
        } finally {
            if (cleanupLocked) {
                getCleanUpLock(entityId).readLock().unlock();
            }

            if (deadlockOccurred) {
                //cleanup on a first level in case of deadlock prevention
                getCurrentThreadLockedEntities().forEach(this::cleanUp);
                lockedEntitiesByThread.remove(Thread.currentThread());
            } else {
                final boolean reentrantMode;
                reentrantMode = cleanUpThreadEntitiesAndGetReentrantMode();

                if (entityLocked && !reentrantMode) {
                    cleanUp(entityId);
                }
            }
        }

        return entityLocked;
    }

    private boolean runExclusive(T entityId, Runnable runnable, Duration duration) {
        boolean entityLocked = false;
        final Lock entityLock = getEntityLock(entityId);

        try {
            if (duration == null) {
                entityLock.lock();
                entityLocked = true;
            } else {
                entityLocked = entityLock.tryLock(duration.toNanos(), TimeUnit.NANOSECONDS);
            }

            if (entityLocked) {
                runnable.run();
            }
        } catch (InterruptedException e) {
            throw new InterruptedEntityLockRuntimeException();
        }  finally {
            if (entityLocked) {
                entityLock.unlock();
            }
        }

        return entityLocked;
    }

    private Lock getEntityLock(T entityId) {
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
            throw new DeadLockPreventException("Trying to take nested non-reentrant lock: " + threadEntities, this);
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
