package com.profclub.storage.exception;

public class StorageItemAlreadyExistsException extends StorageException {

    public StorageItemAlreadyExistsException() {
    }

    public StorageItemAlreadyExistsException(String message) {
        super(message);
    }

    public StorageItemAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageItemAlreadyExistsException(Throwable cause) {
        super(cause);
    }
}
