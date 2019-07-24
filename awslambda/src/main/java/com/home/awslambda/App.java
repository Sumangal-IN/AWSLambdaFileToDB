package com.home.awslambda;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URLDecoder;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.http.client.fluent.Request;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;

public class App implements RequestHandler<S3Event, Void> {

	public Void handleRequest(S3Event s3event, Context context) {
		try {
			// connect to S3
			S3EventNotificationRecord record = s3event.getRecords().get(0);
			Reader reader = new BufferedReader(new InputStreamReader(AmazonS3ClientBuilder.standard().build()
					.getObject(new GetObjectRequest(record.getS3().getBucket().getName(),
							URLDecoder.decode(record.getS3().getObject().getKey().replace('+', ' '), "UTF-8")))
					.getObjectContent()));
			// file processing
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
