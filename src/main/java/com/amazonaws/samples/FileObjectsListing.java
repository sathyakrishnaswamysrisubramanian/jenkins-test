package com.amazonaws.samples;


import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
public class FileObjectsListing {
	private static String source_bucketName = "umg-ers-analytics-dev";
	private static String sourcePath = "qubole/ers-it/AmazonMusicUnlimited20180517/";
	
	public static void main(String[] args) throws IOException {
        AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
        try {
            System.out.println("Listing objects");
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(source_bucketName)
                .withPrefix(sourcePath);
            ObjectListing objectListing;   
            do {
                objectListing = s3client.listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                	String srcKey = objectSummary.getKey();
                	if(objectSummary.getKey().contains(".zip")) {
                		System.out.println(" - " + objectSummary.getKey());
                	}             	
                	
                }
           	
                } while (objectListing.isTruncated());          
        	
       
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
