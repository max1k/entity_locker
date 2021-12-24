package ru.mxk.util;



import javax.annotation.Nonnull;
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
    Object GLOBAL_LOCK_OBJECT = new Object();
    EntityLocker<Object> GLOBAL_LOCK = new ReentrantEntityLocker<>();

    /**
     * Executing runnable if lock by provided entityId is not held.
     * If provided entityId is already locked by another thread
     * then the current thread becomes disabled until the entityId has been unlocked.
     *
     * @param entityId ID of entity to lock with.
     * @param runnable code to run when lock is taken.
     */
    void lockAndRun(@Nonnull T entityId, @Nonnull Runnable runnable);

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
    boolean lockAndRun(@Nonnull T entityId, @Nonnull Runnable runnable, @Nonnull Duration timeout)
            throws InterruptedException;

    /**
     * Executing runnable if global lock is not held.
     * If global lock is held by another thread
     * then the current thread becomes disabled until the global lock has been unlocked.
     *
     * @param runnable code to run when global lock is taken.
     */
    static void lockGlobalAndRun(Runnable runnable) {
        GLOBAL_LOCK.lockAndRun(GLOBAL_LOCK_OBJECT, runnable);
    }
}
