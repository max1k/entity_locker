package ru.mxk.util;

import java.util.Collections;
import java.util.List;

public class DeadLockPreventException extends IllegalMonitorStateException {

    private final String message;
    private final List<?> entities;


    public <T> DeadLockPreventException(String message, List<T> entities) {
        this.message = message;
        this.entities = entities;
    }

    public String getMessage() {
        return message;
    }

    public List<?> getEntities() {
        return Collections.unmodifiableList(entities);
    }
}
