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
public class FilePartitionCopyS3ToS3 {
	private static String source_bucketName = "umg-ers-analytics-dev";
	private static String dest_bucketName = "umg-ers-analytics-dev";
	private static String sourcePath = "qubole/ers-it/AmazonMusicUnlimited20180517/";
	private static String formattedPath = "qubole/ers-it/AmazonMusicUnlimited20180517/formatted_new/";
	
	public static void main(String[] args) throws IOException {
        AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
        try {
            System.out.println("Listing objects");
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(source_bucketName)
                .withPrefix(sourcePath);
            ObjectListing objectListing;   
            String fileTypePartition = "";
            String yearPartition ="";
            String monthPartition = "";
            String dayPartition = "";
            String regPartition = "";
            do {
                objectListing = s3client.listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                	String srcKey = objectSummary.getKey();
                	if(srcKey.contains("ROE_EU") || srcKey.contains("ROW_NA")) {
                	System.out.println(" - " + objectSummary.getKey());
                	String[] split = objectSummary.getKey().split("Amazon_MusicUnlimited_");
                	
                	String fileName = split[1];
                	if(fileName.contains("Cons")) {
                		fileTypePartition="Cons";
                	}else if(fileName.contains("Trans")) {
                		fileTypePartition="Trans";
                	}else if(fileName.contains("Total")) {
                		fileTypePartition="Total";
                	}
                	String[] fileSplit = fileName.split("_");
                	String fileDate = fileSplit[4].replaceAll(".txt.zip", "");
                	regPartition = fileSplit[0]+"_"+fileSplit[1];
                	yearPartition = fileDate.substring(0, 4);
                	monthPartition = fileDate.substring(4, 6);
                	dayPartition = fileDate.substring(6, 8);
                	
                	String destinationPath = formattedPath+fileTypePartition+"/year="+yearPartition+"/month="+monthPartition+"/day="+dayPartition+"/region="+regPartition+"/";
                	String destFileName = "Amazon_Prime_"+fileName;
                	destFileName = destFileName.replaceAll(".txt.zip", ".txt");
                	CopyObjectRequest copyObjRequest = new CopyObjectRequest(
                			source_bucketName, objectSummary.getKey(), dest_bucketName, destinationPath+destFileName);
                	s3client.copyObject(copyObjRequest);                  
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
