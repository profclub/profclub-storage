package com.profclub.storage;

public enum StorageProviderType {

    S3,
    FS;

    public static StorageProviderType getByType(String val) {
        switch (val) {
            case "S3":
            case "s3":
                return StorageProviderType.S3;
            case "fs":
            case "FS":
                return StorageProviderType.FS;
            default:
                return getDefault();
        }
    }

    public static StorageProviderType getDefault() {
        return StorageProviderType.FS;
    }

}
