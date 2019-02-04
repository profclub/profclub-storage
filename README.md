# profclub-storage

#### Supported Providers

	1. AWS : S3
	2. FS  : File System

#### Usage

Maven include
```
<dependency>
	<groupId>profclub</groupId>
	<artifactId>profclub-storage</artifactId>
	<version>1.0-SNAPSHOT</version>
</dependency>

```

There is two ways:

	1. Single Provider
	2. Multiply Proveders
	
1. Single Provider

In configuration class define bean of storage provider

For file system provider the following configuration is needed;

```
@Autovired
private LocalStorageConfiguration localStorageConfiguration;

@Bean
public IStorageProvider storageProvider() {
	return new FileSystemStorageProvider(localStorageConfiguration);
}


```

```
@Bean
class LocalStorageConfigurationImpl implements LocalStorageConfiguration {
	
	@Override
	public String getBasePath() {
		return "TODO: basePath of storage."
	}

}

```
	
For Aws S3 storage provider integration the following configuration is needed:
```
@Autowired
private AWSConfiguration awsConfiguration;

@Autowired
private AmazonS3 s3Client;

@Bean
public IStorageProvider storageProvider() {
	return new AwsStorageProvider (awsConfiguration, s3Client);
}

```

```
@Bean
class AWSConfigurationImpl implements AWSConfiguration {
	
	@Override
	public String getS3BaseBucket() {
		return "TODO: base bucket name of storage."
	}
}
```

### Features:

##### AWS S3 Features
 - upload - Void
 - read	  - InputStream
 - delete - Void
 - move   - Void
 - copy   - Void
 - exist  - Boolean
 - size   - long
 
##### FS Features 
 - upload - Void
 - create - OutputStream 
 - read   - InputStream
 - delete - Void
 - move   - Void
 - copy	  - Void
 - exist  - Boolean
 - size   - long
	