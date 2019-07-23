package com.home.awslambda;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

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
			System.out.println(s3event.getRecords().size());
			S3EventNotificationRecord record = s3event.getRecords().get(0);

			String bucket = record.getS3().getBucket().getName();
			System.out.println("bucket" + bucket);
			String bucketKey = record.getS3().getObject().getKey().replace('+', ' ');
			System.out.println("key" + record.getS3().getObject().getKey());
			bucketKey = URLDecoder.decode(bucketKey, "UTF-8");
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
			S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, bucketKey));

			InputStream in = s3Object.getObjectContent();
			Reader reader = new BufferedReader(new InputStreamReader(in));

			CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
			csvParser.getRecords().stream().forEach(r -> {
				HttpURLConnection conn = null;
				try {
					System.out.println(String.format("https://load-app.herokuapp.com/records/%s/%s", r.get(0), r.get(1)));
					conn = (HttpURLConnection) new URL(
							String.format("https://load-app.herokuapp.com/records/%s/%s", r.get(0), r.get(1)))
									.openConnection();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (conn != null)
						conn.disconnect();
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
