package com.home.awslambda;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;

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
			String bucketKey = record.getS3().getObject().getKey()
					.replace('+', ' ');

			bucketKey = URLDecoder.decode(bucketKey, "UTF-8");

			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
			S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket,
					bucketKey));

			InputStream in = s3Object.getObjectContent();
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
			s3Object.close();

			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = null;
			System.out.println("Trying to connect to DB ....");
			connection = DriverManager
					.getConnection("jdbc:mysql://myinstance.cjotpija7r7c.us-east-1.rds.amazonaws.com:3306/dbname?"
							+ "user=sumangal&password=linuxlinux");
			if (connection != null)
				System.out.println("DB connection sucessful");
			else
				System.out.println("DB connection failure");
			connection.close();
			System.out.println("DB connection closed ....");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "Ok";
	}
}
