package com.profclub.storage;



import com.profclub.storage.exception.*;

import java.io.*;

public interface IStorageProvider {

    void upload(StorageType type, String id, String folderID, byte[] content) throws StorageException;

    OutputStream create(StorageType type, String id, String folderID) throws StorageException;

    InputStream read(StorageType type, String id, String folderID) throws StorageException;

    void delete(StorageType type, String id, String folderID) throws StorageException;

    void move(StorageType fromType, String fromId, StorageType toType, String toId, String folderID) throws StorageException;

    void copy(StorageType fromType, String fromId, StorageType toType, String toId, String folderID) throws StorageException;

    boolean exist(StorageType type, String id, String folderID) throws StorageException;

    long getSize(StorageType type, String id, String folderID) throws StorageException;

    StorageProviderType getProvider();

    boolean supportOutputStream();
}
