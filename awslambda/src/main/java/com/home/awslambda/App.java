package com.home.awslambda;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class App implements RequestHandler<S3Event, String> {
	public String handleRequest(S3Event s3event, Context context) {

		S3EventNotificationRecord record = s3event.getRecords().get(0);

		String bucket = record.getS3().getBucket().getName();
		String bucketKey = record.getS3().getObject().getKey()
				.replace('+', ' ');
		try {
			bucketKey = URLDecoder.decode(bucketKey, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		bucketKey += "_new";

		AmazonS3 s3Client = new AmazonS3Client();
		S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket,
				bucketKey));

		s3Client.putObject(bucket, bucketKey, s3Object.getObjectContent(),
				s3Object.getObjectMetadata());
		return "Ok";
	}
}
