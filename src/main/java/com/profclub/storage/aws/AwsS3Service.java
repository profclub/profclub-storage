package com.profclub.storage.aws;

import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.profclub.common.util.*;
import com.profclub.storage.config.*;
import com.profclub.storage.exception.*;
import org.apache.commons.io.*;
import org.slf4j.*;
import javax.annotation.*;
import java.io.*;
import java.util.*;

/**
 * Implementation of service
 * Amazon S3 integration service for files storage management.
 */
public class AwsS3Service  {

    private static final Logger LOG = LoggerFactory.getLogger(AwsS3Service.class);

    private final String S3_OBJECT_DELIM = "/";

    private String s3BucketName;

    /** S3 Client instance */
    private AmazonS3 s3Client;

    private AWSConfiguration awsConfiguration;

    /**
     * Initializes a new instance of the class.
     */
    public AwsS3Service(AmazonS3 s3Client, AWSConfiguration awsConfiguration) {
        this.s3Client = s3Client;
        this.awsConfiguration = awsConfiguration;
    }

    /**
     * Executes initialization logic.
     */
    @PostConstruct
    private void init() {
        LOG.debug("Creating AWS S3 client");

        this.s3BucketName = awsConfiguration.getS3BaseBucket();
        // acquire list of S3 buckets
        List<String> s3Buckets = null;
        try {
            s3Buckets = listBuckets();
        } catch (StorageException e) {
            throw new RuntimeException("Unexpected exception", e);
        }

        // ensure bucket name specified
        if (StringHelper.isBlank(s3BucketName)) {
            throw new RuntimeException("S3 configuration bucket cannot be empty");
        }

        // ensure configured bucket exists
        boolean exists = false;
        for (String bucketName : s3Buckets) {
            if (bucketName.equals(s3BucketName)) {
                exists = true;
                break;
            }
        }

        // create configured bucket if not found on the cloud
        if (!exists) {
            try {
                createBucket(s3BucketName);
            } catch (StorageException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        LOG.info("AWS S3 manager service initialized.");
    }

    ///////////////////////////////////////////////

    /**
     * Creates a new S3 bucket.
     *
     * @param bucketName
     */
    public void createBucket(String bucketName) throws StorageException{
        try {
            Bucket bucket = s3Client.createBucket(bucketName);
            LOG.info("S3 bucket created: {}", bucket.getName());
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage(), ex);
        }
    }

    /**
     * Creates a new S3 bucket in specified Region.
     *
     * @param bucketName
     * @param region
     */
    public void createBucket(String bucketName, String region) throws StorageException{
        try {
            CreateBucketRequest request = new CreateBucketRequest(bucketName, region);
            Bucket bucket = s3Client.createBucket(request);
            LOG.info("S3 bucket created: {} in region [{}]", bucket.getName(), region);
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage(), ex);
        }
    }

    /**
     * Gets all existing bucket names.
     *
     * @return
     */
    public List<String> listBuckets() throws StorageException {
        List<String> resultList = new ArrayList<>();
        try {
            List<Bucket> s3Buckets = s3Client.listBuckets();
            for (Bucket bucket : s3Buckets) {
                if (bucket.getName().equals(s3BucketName)) {
                    resultList.add(bucket.getName());
                }
            }
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage(), ex);
        }
        return resultList;
    }

    /**
     * Gets names of existing root directories in specified bucket.
     *
     * @param bucketName
     * @return
     */
    public List<String> listFolders(String bucketName) throws StorageException{
        List<String> resultList = new ArrayList<>();
        try {
            // acquire S3 objects response
            ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                    .withDelimiter(S3_OBJECT_DELIM);
            ListObjectsV2Result response = s3Client.listObjectsV2(request);

            // filter folders
            List<String> objectPrefixes = response.getCommonPrefixes();
            if (CollectionHelper.isNotBlank(objectPrefixes)) {
                for (String object : objectPrefixes) {
                    if (object.endsWith(S3_OBJECT_DELIM)) {
                        resultList.add(object.substring(0, object.length() - 1));
                    }
                }
            }
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage(), ex);
        }
        return resultList;
    }

    /**
     * Gets names of existing 1st level directories in specified root directory.
     *
     * @param bucketName
     * @param folderName
     * @return
     */
    public List<String> listSubFolders(String bucketName, String folderName) throws StorageException{
        List<String> resultList = new ArrayList<>();
        String prefix = folderName + S3_OBJECT_DELIM;
        try {
            // acquire S3 objects response
            ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
                    .withPrefix(prefix)
                    .withDelimiter(S3_OBJECT_DELIM);
            ListObjectsV2Result response = s3Client.listObjectsV2(request);

            // filter folders
            List<String> objectPrefixes = response.getCommonPrefixes();
            if (CollectionHelper.isNotBlank(objectPrefixes)) {
                for (String object : objectPrefixes) {
                    if (object.endsWith(S3_OBJECT_DELIM)) {
                        resultList.add(object.substring(prefix.length(), object.length() - 1));
                    }
                }
            }
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage(), ex);
        }
        return resultList;
    }

    /**
     * Gets files/objects in specified root directory.
     *
     * @note: folders are excluded.
     *
     * @param bucketName
     * @param folderName
     * @return
     */
    public List<String> listObjects(String bucketName, String folderName) throws StorageException{
        String prefix = folderName + S3_OBJECT_DELIM;
        return listObjectsWithPrefix(bucketName, prefix);
    }

    /**
     * Gets files/objects in specified subfolder of selected root directory.
     *
     * @param bucketName
     * @param folderName
     * @param subFolder
     * @return
     */
    public List<String> listObjects(String bucketName, String folderName, String subFolder)throws StorageException {
        String prefix = folderName + S3_OBJECT_DELIM + subFolder + S3_OBJECT_DELIM;
        return listObjectsWithPrefix(bucketName, prefix);
    }

    /**
     * Gets files/objects with specified S3 prefix.
     *
     * @param bucketName
     * @param prefix
     * @return
     */
    public List<String> listObjectsWithPrefix(String bucketName, String prefix) throws StorageException{
        try {
            // acquire object summaries
            List<S3ObjectSummary> s3ObjectSummaries = getObjectSummaries(bucketName, prefix);

            // filter directories
            filterS3Folders(s3ObjectSummaries);

            // extract object names
            return getObjectNames(prefix, s3ObjectSummaries);
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage(), ex);
        }
    }

    /**
     * Gets all objects in specified S3 bucket.
     *
     * @param bucketName
     * @return
     */
    public List<String> listAll(String bucketName) throws StorageException{
        List<String> resultList = new ArrayList<>();
        try {
            // acquire S3 objects response
            ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName);
            ListObjectsV2Result response = s3Client.listObjectsV2(request);

            // extract object names
            List<S3ObjectSummary> s3ObjectSummaries = response.getObjectSummaries();
            if (CollectionHelper.isNotBlank(s3ObjectSummaries)) {
                for (S3ObjectSummary summary : s3ObjectSummaries) {
                    resultList.add(summary.getKey());
                }
            }
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage(), ex);
        }
        return resultList;
    }

    /**
     * Checks if an object (file) with specified name exists in selected root directory
     * of the specified S3 bucket.
     *
     * @param bucketName
     * @param folderName
     * @param fileName
     * @return
     */
    public boolean exists(String bucketName, String folderName, String fileName) throws StorageException{
        String objectKey = StringHelper.isBlank(folderName)
                ? fileName
                : folderName + S3_OBJECT_DELIM + fileName;
        return exists(bucketName, objectKey);
    }

    /**
     * Checks if an object (file) with specified name exists in selected sub-folder
     * of the specified root directory.
     *
     * @param bucketName
     * @param folderName
     * @param subFolder
     * @param fileName
     * @return
     */
    public boolean exists(String bucketName, String folderName, String subFolder, String fileName) throws StorageException{
        String objectKey = StringHelper.isBlank(folderName)
                ? fileName
                : folderName + S3_OBJECT_DELIM + subFolder + S3_OBJECT_DELIM + fileName;
        return exists(bucketName, objectKey);
    }

    /**
     * Checks if an S3 object exists with specified KEY.
     *
     * @param bucketName
     * @param s3ObjectKey
     * @return
     */
    public boolean exists(String bucketName, String s3ObjectKey) throws StorageException{
        try {
            return s3Client.doesObjectExist(bucketName, s3ObjectKey);
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage(), ex);
        }
    }

    /**
     * Creates a root directory in specified S3 bucket.
     *
     * @param bucketName
     * @param folderName
     */
    public void createFolder(String bucketName, String folderName) throws StorageException{
        String s3FolderKey = folderName + S3_OBJECT_DELIM;
        createS3FolderByKey(bucketName, s3FolderKey);
    }

    /**
     * Creates a sub-folder in specified root directory.
     *
     * @param bucketName
     * @param folderName
     * @param subFolder
     */
    public void createSubFolder(String bucketName, String folderName, String subFolder) throws StorageException{
        String s3FolderKey = folderName + S3_OBJECT_DELIM + subFolder + S3_OBJECT_DELIM;
        createS3FolderByKey(bucketName, s3FolderKey);
    }

    /**
     * Uploads a byte[] content as a file, to the specified root directory of the S3 bucket.
     *
     * @param bucketName
     * @param folderName
     * @param fileName
     * @param fileContent
     */
    public void uploadObject(String bucketName, String folderName, String fileName, byte[] fileContent)throws StorageException {
        String objectKey = StringHelper.isBlank(folderName) ? fileName : folderName + S3_OBJECT_DELIM + fileName;
        uploadObject(bucketName, objectKey, fileContent);
    }

    /**
     * Uploads selected file to specified root directory of the S3 bucket.
     *
     * @param bucketName
     * @param folderName
     * @param file
     */
    public void uploadFile(String bucketName, String folderName, File file) throws StorageException{
        String objectKey = StringHelper.isBlank(folderName) ? file.getName() : folderName + S3_OBJECT_DELIM + file.getName();
        uploadObject(bucketName, objectKey, file);
    }

    /**
     * Uploads a file using the S3 KEY.
     *
     * @param bucketName
     * @param s3ObjectKey
     * @param file
     */
    public void uploadObject(String bucketName, String s3ObjectKey, File file) throws StorageException{
        try {
            PutObjectRequest request = new PutObjectRequest(bucketName, s3ObjectKey, file);

            // upload file
            PutObjectResult response = s3Client.putObject(request);

            LOG.debug("File {} uploaded. [MD5: {}]", file.getAbsolutePath(), response.getContentMd5());
        } catch (Exception ex) {
            throw new StorageException(String.format("Key: %s, Error: %s", s3ObjectKey, ex.getMessage()), ex);
        }
    }

    /**
     * Uploads a file content using the S3 KEY.
     *
     * @param bucketName
     * @param s3ObjectKey
     * @param fileContent
     */
    public void uploadObject(String bucketName, String s3ObjectKey, byte[] fileContent) throws StorageException{
        try {
            // upload file as byte array
            PutObjectResult response = s3Client.putObject(bucketName, s3ObjectKey,
                    new ByteArrayInputStream(fileContent), new ObjectMetadata());

            LOG.debug("Object [key: {}] uploaded. [MD5: {}]", s3ObjectKey, response.getContentMd5());
        } catch (Exception ex) {
            throw new StorageException(String.format("Key: %s, Error: %s", s3ObjectKey, ex.getMessage()), ex);
        }
    }

    /**
     * Gets selected file content from specified root directory.
     *
     * @param bucketName
     * @param folderName
     * @param fileName
     * @return
     */
    public byte[] getObjectContent(String bucketName, String folderName, String fileName) throws StorageException{
        String objectKey = StringHelper.isBlank(folderName)
                ? fileName
                : folderName + S3_OBJECT_DELIM + fileName;
        return getObjectContent(bucketName, objectKey);
    }

    /**
     * Gets selected file content from specified root directory.
     *
     * @param bucketName
     * @param folderName
     * @param fileName
     * @return
     */
    public byte[] getObjectContent(String bucketName, String folderName, String subFolder, String fileName) throws StorageException{
        String objectKey = StringHelper.isBlank(folderName)
                ? fileName
                : folderName + S3_OBJECT_DELIM + subFolder + S3_OBJECT_DELIM + fileName;
        return getObjectContent(bucketName, objectKey);
    }

    /**
     * Gets selected file content by its S3 KEY.
     *
     * @param bucketName
     * @param s3ObjectKey
     * @return
     */
    public byte[] getObjectContent(String bucketName, String s3ObjectKey) throws StorageException{
        try {
            byte[] content = IOUtils.toByteArray(getObjectContentAsStream(bucketName, s3ObjectKey));
            LOG.debug("S3 object [key: {}] content retrieved (length: {})", s3ObjectKey, content.length);
        return content;
        } catch (Exception ex) {
            throw new StorageException(String.format("Key: %s, Error: %s", s3ObjectKey, ex.getMessage()), ex);
        }
    }

    public InputStream getObjectContentAsStream(String bucketName, String folderName, String subFolder, String fileName) throws StorageException{
        String objectKey = StringHelper.isBlank(folderName)
                ? fileName
                : folderName + S3_OBJECT_DELIM + subFolder + S3_OBJECT_DELIM + fileName;
        return getObjectContentAsStream(bucketName, objectKey);
    }

    /**
     * Gets selected file content by its S3 KEY.
     *
     * @param bucketName
     * @param s3ObjectKey
     * @return
     */
    public InputStream getObjectContentAsStream(String bucketName, String s3ObjectKey) throws StorageException{
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, s3ObjectKey);
            S3Object s3Object = s3Client.getObject(request);
            return  s3Object.getObjectContent();
        } catch (Exception ex) {
            throw new StorageException(String.format("Key: %s, Error: %s", s3ObjectKey, ex.getMessage()), ex);
        }
    }

    /**
     * Gets selected File from specified root directory.
     *
     * @param bucketName
     * @param folderName
     * @param fileName
     * @return
     */
    public File getAsFile(String bucketName, String folderName, String fileName) throws StorageException{
        String objectKey = StringHelper.isBlank(folderName) ? fileName : folderName + S3_OBJECT_DELIM + fileName;
        try {
            // remove old file from temp dir of exists
            File f = new File(System.getProperty("java.io.tmpdir") + File.separator + fileName);
            if (f.exists()) {
                FileUtils.forceDelete(f);
            }

            // get file content
            GetObjectRequest request = new GetObjectRequest(bucketName, objectKey);
            S3Object s3Object = s3Client.getObject(request);
            InputStream stream = s3Object.getObjectContent();

            // create/return a file in temp dir
            if (f.createNewFile()) {
                FileUtils.copyToFile(stream, f);
                return f;
            } else {
                throw new StorageException("Unable to create file {" +  f.getAbsolutePath() + "}");
            }
        } catch (Exception ex) {
            throw new StorageException(String.format("Key: %s, Error: %s", objectKey, ex.getMessage()), ex);
        }
    }

    /**
     * Deletes an S3 object from root directory.
     *
     * @param bucketName
     * @param folderName
     * @param fileName
     */
    public void deleteObject(String bucketName, String folderName, String fileName) throws StorageException{
        String objectKey = StringHelper.isBlank(folderName) ? fileName : folderName + S3_OBJECT_DELIM + fileName;
        deleteObject(bucketName, objectKey);
    }

    /**
     * Deletes an S3 object by it's KEY.
     *
     * @param bucketName
     * @param s3ObjectKey
     */
    public void deleteObject(String bucketName, String s3ObjectKey) throws StorageException{
        try {
            if (s3ObjectKey.endsWith(S3_OBJECT_DELIM)) {
                String directoryName = s3ObjectKey;
                if (s3ObjectKey.indexOf(S3_OBJECT_DELIM) != s3ObjectKey.lastIndexOf(S3_OBJECT_DELIM)) {
                    String folderPath = s3ObjectKey.substring(0, s3ObjectKey.length() - 1);
                    String[] pathArray = folderPath.split(S3_OBJECT_DELIM);
                    directoryName = pathArray[pathArray.length - 1] + S3_OBJECT_DELIM;
                }

                List<String> files = listObjects(bucketName, s3ObjectKey);
                if (CollectionHelper.isNotBlank(files)) {
                    throw new StorageException("Directory is not blank" + directoryName);
                }
            }
            s3Client.deleteObject(bucketName, s3ObjectKey);
        } catch (Exception ex) {
            throw new StorageException(String.format("Key: %s, Error: %s", s3ObjectKey, ex.getMessage()), ex);
        }
    }

    // region <HELPERS>

    private List<S3ObjectSummary> getObjectSummaries(String s3BucketName, String prefix) {
        // create list object request
        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(s3BucketName)
                .withPrefix(prefix)
                .withDelimiter(S3_OBJECT_DELIM);

        // acquire response
        ListObjectsV2Result response = s3Client.listObjectsV2(request);

        // extract/return object summaries
        return response.getObjectSummaries();
    }

    private void filterS3Folders(List<S3ObjectSummary> s3ObjectSummaries) {
        if (CollectionHelper.isNotBlank(s3ObjectSummaries)) {
            for (Iterator<S3ObjectSummary> iterator = s3ObjectSummaries.iterator(); iterator.hasNext();) {
                S3ObjectSummary objectSummary = iterator.next();

                if (objectSummary.getKey().endsWith(S3_OBJECT_DELIM)) {
                    iterator.remove();
                }
            }
        }
    }

    private List<String> getObjectNames(String prefix, List<S3ObjectSummary> objectSummaries) {
        List<String> resultList = new ArrayList<>();

        for (S3ObjectSummary objectSummary : objectSummaries) {
            String objectName = objectSummary.getKey().substring(prefix.length(), objectSummary.getKey().length() - 1);
            resultList.add(objectName);
        }

        return resultList;
    }

    private void createS3FolderByKey(String s3BucketName, String s3ObjectKey) throws StorageException{
        try {
            // create meta-data for the folder and set content-length to 0
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(0);

            // create empty content
            InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

            // create a PutObjectRequest passing the folder name suffixed by /
            PutObjectRequest putObjectRequest = new PutObjectRequest(s3BucketName, s3ObjectKey, emptyContent, metadata);

            // send request to S3 to create folder
            s3Client.putObject(putObjectRequest);
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage(), ex);
        }
    }

    // endregion

}
