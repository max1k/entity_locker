package ru.mxk.util;

import java.time.Duration;

/**
 * Synchronization mechanism similar to row-level DB locking.
 * Supposed to be used by the components that are responsible for managing storage
 * and caching of different type of entities in the application.
 *
 * For given entityId, guarantees that at most one thread executes protected code on that entity.
 * If there’s a concurrent request to lock the same entity, the other thread should wait until the entity becomes available
 * @param <T>
 */
public interface EntityLocker<T> {
    /**
     * Executing runnable if lock by provided entityId is not held.
     * If provided entityId is already locked by another thread
     * then the current thread becomes disabled until the entityId has been unlocked.
     *
     * @param entityId ID of entity to lock with.
     * @param runnable code to run when lock is taken.
     */
    void lockAndRun(T entityId, Runnable runnable);

    /**
     * Executing runnable if lock by provided entityId is not held and returning {@code true}.
     * If the lock by entityId is held then the current thread becomes disabled until one of following things happens:
     * * The lock by entityId is acquired by the current thread
     * * The specified waiting time elapses
     *
     * @param entityId ID of entity to lock with.
     * @param runnable code to run when lock is taken.
     * @param timeout the time to wait for the lock
     */
    boolean lockAndRun(T entityId, Runnable runnable, Duration timeout) throws InterruptedException;
}
