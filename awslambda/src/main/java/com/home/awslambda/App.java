package com.home.awslambda;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URLDecoder;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.http.client.fluent.Request;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class App implements RequestHandler<S3Event, String> {

	public String handleRequest(S3Event s3event, Context context) {

		try {
			S3EventNotificationRecord record = s3event.getRecords().get(0);
			String bucket = record.getS3().getBucket().getName();
			String bucketKey = record.getS3().getObject().getKey().replace('+', ' ');
			bucketKey = URLDecoder.decode(bucketKey, "UTF-8");
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
			S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, bucketKey));

			InputStream in = s3Object.getObjectContent();
			Reader reader = new BufferedReader(new InputStreamReader(in));

			CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
			csvParser.getRecords().stream().forEach(r -> {
				try {
					Request.Post(String.format("https://load-app.herokuapp.com/records/%s/%s", r.get(0), r.get(1)))
							.execute();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			csvParser.close();

			return "Done";
		} catch (Exception e) {
			e.printStackTrace();
			return "Failed";
		}

	}

}
