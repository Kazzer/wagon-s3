package com.allya.maven.wagon.providers.s3;

import static java.lang.Boolean.TRUE;
import static software.amazon.awssdk.regions.Region.US_EAST_1;
import static software.amazon.awssdk.regions.Region.of;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;

import org.apache.maven.wagon.AbstractWagon;
import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.resource.Resource;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.Download;
import software.amazon.awssdk.transfer.s3.DownloadRequest;
import software.amazon.awssdk.transfer.s3.S3ClientConfiguration;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.Upload;
import software.amazon.awssdk.transfer.s3.UploadRequest;

// CHECKSTYLE.OFF: ClassDataAbstractionCoupling
public final class S3Wagon extends AbstractWagon {
    // CHECKSTYLE.ON: ClassDataAbstractionCoupling
    private static final String PROPERTY_KEY_ACL = "maven.wagon.s3.acl";
    private static final String PROPERTY_KEY_REGION = "maven.wagon.s3.region";

    private S3Client s3;
    private String bucket;
    private String prefix;
    private ObjectCannedACL acl;
    private S3TransferManager transferManager;

    @Override
    protected void closeConnection() {
        transferManager.close();

        fireSessionLoggedOff();

        s3.close();
    }

    @Override
    public void get(final String resourceName, final File destination)
        throws TransferFailedException, ResourceDoesNotExistException {
        final Resource resource = new Resource(resourceName);

        fireGetInitiated(resource, destination);

        getObject(resource, destination);
    }

    @Override
    public boolean getIfNewer(final String resourceName, final File destination, final long timestamp)
        throws TransferFailedException, ResourceDoesNotExistException {
        final Resource resource = new Resource(resourceName);

        fireGetInitiated(resource, destination);

        final HeadObjectResponse metadata = s3.headObject(HeadObjectRequest
            .builder()
            .bucket(bucket)
            .key(String.join("", prefix, resourceName))
            .build());
        final boolean destinationIsNewer = metadata.lastModified().isAfter(Instant.ofEpochMilli(timestamp));
        if (destinationIsNewer) {
            getObject(resource, destination);
        }

        return destinationIsNewer;
    }

    @Override
    protected void openConnectionInternal() throws ConnectionException, AuthenticationException {
        bucket = getRepository().getHost();
        s3 = S3Client
            .builder()
            .credentialsProvider(DefaultCredentialsProvider.create())
            .region(of(System.getProperty(PROPERTY_KEY_REGION, US_EAST_1.id())))
            .serviceConfiguration(S3Configuration
                .builder()
                .multiRegionEnabled(TRUE)
                .useArnRegionEnabled(TRUE)
                .build())
            .build();
        transferManager = S3TransferManager
            .builder()
            .s3ClientConfiguration(S3ClientConfiguration
                .builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(of(System.getProperty(PROPERTY_KEY_REGION, US_EAST_1.id())))
                .build())
            .build();

        fireSessionLoggedIn();

        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        }
        catch (final NoSuchBucketException nsbe) {
            throw new ConnectionException(String.format("Bucket '%s' does not exist", bucket));
        }
        catch (final AwsServiceException ase) {
            throw new AuthenticationException(
                String.format("Insufficient permissions to access bucket '%s'", bucket),
                ase);
        }

        acl = Arrays
            .stream(ObjectCannedACL.values())
            .filter(cacl -> cacl.toString().equals(System.getProperty(PROPERTY_KEY_ACL)))
            .findFirst()
            .orElse(null);
        prefix = getPrefix(getRepository().getBasedir());
    }

    @Override
    public void put(final File source, final String destination) throws TransferFailedException {
        final Resource resource = new Resource(destination);

        firePutInitiated(resource, source);

        final String key;
        key = String.join("", prefix, destination);

        Upload upload = null;
        try {
            upload = transferManager.upload(UploadRequest
                .builder()
                .putObjectRequest(PutObjectRequest
                    .builder()
                    .acl(acl)
                    .bucket(bucket)
                    .contentLength(source.length())
                    .key(key)
                    .build())
                .source(source)
                .build());
            upload.completionFuture().join();
        }
        catch (final AwsServiceException ase) {
            fireTransferError(resource, ase, TransferEvent.REQUEST_PUT);

            throw new TransferFailedException(String.format("Unable to upload '%s'", source.getName()), ase);
        }

        postProcessListeners(resource, source, TransferEvent.REQUEST_PUT);

        firePutCompleted(resource, source);
    }

    private void getObject(final Resource source, final File destination)
        throws TransferFailedException, ResourceDoesNotExistException {
        fireGetStarted(source, destination);

        final String key = String.join("", prefix, source.getName());

        try {
            s3.headObject(HeadObjectRequest
                .builder()
                .bucket(bucket)
                .key(key)
                .build());
        }
        catch (final NoSuchKeyException nske) {
            fireTransferError(source, null, TransferEvent.REQUEST_GET);

            throw new ResourceDoesNotExistException(String.format(
                "Resource '%s' does not exist in bucket '%s'",
                source.getName(),
                bucket));
        }

        Download download = null;
        try {
            // File cannot be overwritten yet: https://github.com/aws/aws-sdk-java-v2/pull/2595
            if (destination.exists()) {
                if (!destination.delete()) {
                    fireTransferError(source, null, TransferEvent.REQUEST_GET);
                    throw new TransferFailedException(
                        String.format("File '%s' already exists and cannot be deleted", destination.getPath()));
                }
            }

            download = transferManager.download(DownloadRequest
                .builder()
                .destination(destination)
                .getObjectRequest(GetObjectRequest
                    .builder()
                    .bucket(bucket)
                    .key(key)
                    .build())
                .build());
            download.completionFuture().join();
        }
        catch (final AwsServiceException ase) {
            fireTransferError(source, ase, TransferEvent.REQUEST_GET);

            throw new TransferFailedException(String.format("Unable to download '%s'", source.getName()), ase);
        }

        postProcessListeners(source, destination, TransferEvent.REQUEST_GET);

        fireGetCompleted(source, destination);
    }

    private static String getPrefix(final String baseDir) {
        final String separator = "/";
        final String prefix = baseDir.startsWith(separator) ? baseDir.substring(1) : baseDir;
        return prefix.endsWith(separator) ? prefix : String.format("%s/", prefix);
    }
}
