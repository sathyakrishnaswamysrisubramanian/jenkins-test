package com.amazonaws.samples;


import java.io.IOException;




import org.apache.commons.lang.StringUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
public class FileRenameS3ToS3 {
	private static String source_bucketName = "umg-ers-analytics-dev";
	private static String dest_bucketName = "umg-ers-analytics-dev";
	private static String sourcePath = "qubole/ers-it/concord/spotify/v2/historical_may2/users/year=2018/";
	private static String historyPath = "qubole/ers-it/concord/spotify/v2/historical_may2/consolidated/";
	
	public static void main(String[] args) throws IOException {
        AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
        try {
            System.out.println("Listing objects");
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(source_bucketName)
                .withPrefix(sourcePath);
            ObjectListing objectListing;       
            int yearPosition = 7;
            int monthPosition = yearPosition+1;
            int dayPosition = monthPosition+1;
            do {
                objectListing = s3client.listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                	System.out.println(" - " + objectSummary.getKey());
                	String[] split = objectSummary.getKey().split("/");
        			if(split[yearPosition].split("=")[0].contentEquals("year")&&split[monthPosition].split("=")[0].contentEquals("month")&&split[dayPosition].split("=")[0].contentEquals("day")){
        				
        			System.out.println(split[yearPosition].split("=")[1]+split[monthPosition].split("=")[1]+split[dayPosition].split("=")[1]);	
        			//String fileName = "spotify_tracks_"+split[7].split("=")[1]+split[7].split("=")[1]+split[7].split("=")[1]+".txt";
        			String fileDate = split[yearPosition].split("=")[1]+StringUtils.leftPad(split[monthPosition].split("=")[1],2,'0')+StringUtils.leftPad(split[dayPosition].split("=")[1],2,'0');
		        	String fileName = "concord_spotify_users_"+fileDate+".txt.gz";
                	CopyObjectRequest copyObjRequest = new CopyObjectRequest(
                			source_bucketName, objectSummary.getKey(), dest_bucketName, historyPath+fileDate+"/"+fileName);
                	s3client.copyObject(copyObjRequest);
                    
//                    System.out.println("@@@@@@@@"+historyPath+k[r]);
            } 
        			}
//                listObjectsRequest.setMarker(objectListing.getNextMarker());
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
