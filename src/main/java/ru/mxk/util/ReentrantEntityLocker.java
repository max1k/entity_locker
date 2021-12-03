package ru.mxk.util;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReentrantEntityLocker<T> implements EntityLocker<T> {
    private final Map<T, Lock> lockByID = new ConcurrentHashMap<>();

    @Override
    public void lockAndRun(T entityId, Runnable runnable) {
        final Lock lock = getLock(entityId);
        lock.lock();
        runAndUnlock(runnable, lock);
    }

    @Override
    public boolean lockAndRun(T entityId, Runnable runnable, Duration duration) throws InterruptedException {
        final Lock lock = getLock(entityId);
        if (lock.tryLock(duration.toNanos(), TimeUnit.NANOSECONDS)) {
            runAndUnlock(runnable, lock);
            return true;
        }

        return false;
    }

    private Lock getLock(T entityId) {
        return lockByID.computeIfAbsent(entityId, t -> new ReentrantLock());
    }

    private void runAndUnlock(Runnable runnable, Lock lock) {
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }
}
