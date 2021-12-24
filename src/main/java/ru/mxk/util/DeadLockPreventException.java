package ru.mxk.util;

public class DeadLockPreventException extends IllegalMonitorStateException {

    public DeadLockPreventException(String message) {
        super(message);
    }

}
