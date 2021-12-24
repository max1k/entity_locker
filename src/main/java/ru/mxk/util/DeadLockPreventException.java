package ru.mxk.util;

public class DeadLockPreventException extends IllegalMonitorStateException {

    private final EntityLocker<?> locker;


    public DeadLockPreventException(String message, EntityLocker<?> locker) {
        super(message);
        this.locker = locker;

    }

    public EntityLocker<?> getLocker() {
        return locker;
    }
}
