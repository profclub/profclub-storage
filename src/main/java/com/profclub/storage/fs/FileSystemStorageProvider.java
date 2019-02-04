package com.profclub.storage.fs;

import com.profclub.storage.*;
import com.profclub.storage.config.*;
import com.profclub.storage.exception.*;
import javax.annotation.*;
import java.io.*;
import java.nio.file.*;

public class FileSystemStorageProvider implements IStorageProvider {

    private LocalStorageConfiguration localStorageConfiguration;

    private Path basePath;

    public FileSystemStorageProvider(LocalStorageConfiguration localStorageConfiguration) {
        this.localStorageConfiguration = localStorageConfiguration;
    }

    @PostConstruct
    public void init() {
        this.basePath = Paths.get(localStorageConfiguration.getBasePath());
    }

    @Override
    public void upload(StorageType type, String id, String folderID, byte[] content) throws StorageException {
        try(OutputStream out = create(type, id, folderID)){
            out.write(content);
        } catch (IOException e) {
            throw new StorageException(e);
        }

    }

    @Override
    public OutputStream create(StorageType type, String id, String folderID) throws StorageException {
        Path filePath = getFilePath(type, id, folderID);

        if (Files.exists(filePath)) {
            throw new StorageItemAlreadyExistsException(filePath.toString() + " Already exists");
        }

        try {
            createParentDirectories(filePath);
            return Files.newOutputStream(filePath);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public InputStream read(StorageType type, String id, String folderID) throws StorageException {
        Path filePath = getFilePath(type, id, folderID);
        try {
            return Files.newInputStream(filePath);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void delete(StorageType type, String id, String folderID) throws StorageException {
        Path filePath = getFilePath(type, id, folderID);

        if (Files.notExists(filePath)) {
            throw new StorageItemAlreadyExistsException(filePath.toString() + " does not exist");
        }

        try {
            Files.delete(filePath);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void move(StorageType fromType, String fromId, StorageType toType, String toId, String folderID) throws StorageException {

    }

    @Override
    public void copy(StorageType fromType, String fromId, StorageType toType, String toId, String folderID) throws StorageException {

    }

    @Override
    public boolean exist(StorageType type, String id, String folderID) throws StorageException {
        try {
            Path filePath = getFilePath(type, id, folderID);
            return Files.exists(filePath);
        } catch(StorageException e) {
            return false;
        }
    }

    @Override
    public long getSize(StorageType type, String id, String folderID) throws StorageException {
        Path filePath = getFilePath(type, id, folderID);
        if (Files.notExists(filePath)) {
            throw new StorageItemAlreadyExistsException(filePath.toString() + " does not exist");
        }
        try {
            return Files.size(filePath);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public StorageProviderType getProvider() {
        return StorageProviderType.FS;
    }

    @Override
    public boolean supportOutputStream() {
        return true;
    }

    private void createParentDirectories(Path path) throws IOException {
        try {
            Files.createDirectories(path.getParent());
        } catch (FileAlreadyExistsException e) {
            //ignore
        }
    }

    private Path getFilePath(StorageType type, String id, String folderID) throws StorageException {
        //validate id

        if (folderID != null) {
            return Paths.get(basePath.toString())
                    .resolve(folderID)
                    .resolve(type.name())
                    .resolve(id);
        } else {
            return Paths.get(basePath.toString())
                    .resolve(type.name())
                    .resolve(id);
        }
    }
}
