package com.allya.maven.wagon.providers.s3;

import static com.amazonaws.regions.Regions.US_EAST_1;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.maven.wagon.AbstractWagon;
import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.resource.Resource;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

// CHECKSTYLE.OFF: ClassDataAbstractionCoupling
public final class S3Wagon extends AbstractWagon {
    // CHECKSTYLE.ON: ClassDataAbstractionCoupling
    private static final String PROPERTY_KEY_ACL = "maven.wagon.s3.acl";

    private AmazonS3 s3;
    private String bucket;
    private String prefix;
    private CannedAccessControlList acl;
    private TransferManager transferManager;

    @Override
    protected void closeConnection() {
        transferManager.shutdownNow();

        fireSessionLoggedOff();

        s3.shutdown();
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

        final ObjectMetadata metadata = s3.getObjectMetadata(bucket, String.join("", prefix, resourceName));
        final boolean destinationIsNewer = metadata.getLastModified().compareTo(new Date(timestamp)) < 0;
        if (destinationIsNewer) {
            getObject(resource, destination);
        }

        return destinationIsNewer;
    }

    @Override
    protected void openConnectionInternal() throws ConnectionException, AuthenticationException {
        bucket = getRepository().getHost();
        s3 = AmazonS3ClientBuilder
            .standard()
            .withRegion(US_EAST_1)
            .withForceGlobalBucketAccessEnabled(true)
            .withCredentials(new DefaultAWSCredentialsProviderChain())
            .build();
        transferManager = TransferManagerBuilder.standard().withS3Client(s3).build();

        fireSessionLoggedIn();

        if (!s3.doesBucketExistV2(bucket)) {
            throw new ConnectionException(String.format("Bucket '%s' does not exist", bucket));
        }

        try {
            s3.getBucketAcl(bucket);
        }
        catch (final AmazonServiceException ase) {
            throw new AuthenticationException(
                String.format("Insufficient permissions to access bucket '%s'", bucket),
                ase);
        }

        acl = Arrays
            .stream(CannedAccessControlList.values())
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

        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(source.length());
        Upload upload = null;
        try {
            upload = transferManager.upload(new PutObjectRequest(bucket, key, source)
                .withCannedAcl(acl)
                .withMetadata(metadata));
            upload.waitForCompletion();
        }
        catch (final AmazonServiceException ase) {
            fireTransferError(resource, ase, TransferEvent.REQUEST_PUT);

            throw new TransferFailedException(String.format("Unable to upload '%s'", source.getName()), ase);
        }
        catch (final InterruptedException ioe) {
            upload.abort();
        }

        postProcessListeners(resource, source, TransferEvent.REQUEST_PUT);

        firePutCompleted(resource, source);
    }

    private void getObject(final Resource source, final File destination)
        throws TransferFailedException, ResourceDoesNotExistException {
        fireGetStarted(source, destination);

        final String key = String.join("", prefix, source.getName());

        if (!s3.doesObjectExist(bucket, key)) {
            fireTransferError(source, null, TransferEvent.REQUEST_GET);

            throw new ResourceDoesNotExistException(String.format(
                "Resource '%s' does not exist in bucket '%s'",
                source.getName(),
                bucket));
        }

        Download download = null;
        try {
            download = transferManager.download(bucket, key, destination);
            download.waitForCompletion();
        }
        catch (final AmazonServiceException ase) {
            fireTransferError(source, ase, TransferEvent.REQUEST_GET);

            throw new TransferFailedException(String.format("Unable to download '%s'", source.getName()), ase);
        }
        catch (final InterruptedException ie) {
            try {
                download.abort();
            }
            catch (final IOException ioe) {
                download.pause();
            }
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
