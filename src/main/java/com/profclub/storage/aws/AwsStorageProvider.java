package com.profclub.storage.aws;

import com.amazonaws.services.s3.*;
import com.profclub.storage.*;
import com.profclub.storage.config.*;
import com.profclub.storage.exception.*;

import java.io.*;

public class AwsStorageProvider implements IStorageProvider {

    private AwsS3Service awsS3Service;

    private AWSConfiguration awsConfiguration;

    public AwsStorageProvider (AWSConfiguration awsConfiguration, AmazonS3 s3Client) {
        this.awsConfiguration = awsConfiguration;
        this.awsS3Service = new AwsS3Service(s3Client, awsConfiguration);
    }

    @Override
    public void upload(StorageType type, String id, String folderID, byte[] content) throws StorageException {
        String folder = type.name() + "/" + folderID;
        awsS3Service.uploadObject(awsConfiguration.getS3BaseBucket(), folder, id, content);
    }

    @Override
    public OutputStream create(StorageType type, String id, String folderID) throws StorageException {
        throw new UnsupportedOperationException("try to use different method aws s3 does not provide out stream");
    }

    @Override
    public InputStream read(StorageType type, String id, String folderID) throws StorageException {
        return awsS3Service.getObjectContentAsStream(awsConfiguration.getS3BaseBucket(), type.name(), folderID, id);
    }

    @Override
    public void delete(StorageType type, String id, String folderID) throws StorageException {
        String folder = type.name() + "/" + folderID;
        awsS3Service.deleteObject(awsConfiguration.getS3BaseBucket(), folder, id);
    }

    @Override
    public void move(StorageType fromType, String fromId, StorageType toType, String toId, String folderID) throws StorageException {

    }

    @Override
    public void copy(StorageType fromType, String fromId, StorageType toType, String toId, String folderID) throws StorageException {

    }

    @Override
    public boolean exist(StorageType type, String id, String folderID) throws StorageException {
        return awsS3Service.exists(awsConfiguration.getS3BaseBucket(), type.name(), folderID, id);
    }

    @Override
    public long getSize(StorageType type, String id, String folderID) throws StorageException {
        return 0;
    }

    @Override
    public StorageProviderType getProvider() {
        return StorageProviderType.S3;
    }

    @Override
    public boolean supportOutputStream() {
        return false;
    }
}
