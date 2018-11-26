package com.amazonaws.samples;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FilenameUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
public class FileDecompressCopyS3ToS3New {
	private static String source_bucketName = "umg-ers-analytics-dev";
	private static String dest_bucketName = "umg-ers-analytics-dev";
	private static String sourcePath = "qubole/ers-it/AmazonMusicUnlimited20180517/";
	private static String decompressedPath = "qubole/ers-it/AmazonMusicUnlimited20180517/";
	
	public static void main(String[] args) throws IOException {
        AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
        try {
        	   ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                       .withBucketName(source_bucketName)
                       .withPrefix(sourcePath);
        	  ObjectListing objectListing;  
        	 objectListing = s3client.listObjects(listObjectsRequest);
             for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
             	String srcKey = objectSummary.getKey();

        	
          	 AmazonS3 s3Client = new AmazonS3Client();
               S3Object s3Object = s3Client.getObject(new GetObjectRequest(source_bucketName, srcKey));
               ZipInputStream zis = new ZipInputStream(s3Object.getObjectContent());
               ZipEntry entry = zis.getNextEntry();
               byte[] buffer = new byte[1024];
               
               if(srcKey.contains("Amazon_MusicUnlimited_ROE_EU_UMG_Cons_20180517.txt.zip")) {
               while(entry != null) {
                   String compressfileName = entry.getName();
                   
                   String mimeType = FileMimeType.fromExtension(FilenameUtils.getExtension(compressfileName)).mimeType();
                   System.out.println("Extracting " + compressfileName + ", compressed: " + entry.getCompressedSize() + " bytes, extracted: " + entry.getSize() + " bytes, mimetype: " + mimeType);
                   ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                   int len;
                   while ((len = zis.read(buffer)) > 0) {
                       outputStream.write(buffer, 0, len);
                   }
                   InputStream is = new ByteArrayInputStream(outputStream.toByteArray());
                   ObjectMetadata meta = new ObjectMetadata();
                   meta.setContentLength(outputStream.size());
                   meta.setContentType(mimeType);
                   s3Client.putObject(dest_bucketName, decompressedPath + compressfileName, is, meta);
                   is.close();
                   outputStream.close();
                   entry = zis.getNextEntry();
                   }
               
               zis.closeEntry();
               zis.close();
               
               System.out.println("Deleting zip file " + source_bucketName + "/" + srcKey + "...");
               s3Client.deleteObject(new DeleteObjectRequest(source_bucketName, srcKey));
               System.out.println("Done deleting");
               }
               
        }
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, " +
            		"which means the client encountered " +
                    "an internal error while trying to communicate" +
                    " with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}
