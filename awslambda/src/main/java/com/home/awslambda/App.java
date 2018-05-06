package com.home.awslambda;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;

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
		try {
			S3EventNotificationRecord record = s3event.getRecords().get(0);

			String bucket = record.getS3().getBucket().getName();
			String bucketKey = record.getS3().getObject().getKey()
					.replace('+', ' ');

			bucketKey = URLDecoder.decode(bucketKey, "UTF-8");

			bucketKey += "_new";

			AmazonS3 s3Client = new AmazonS3Client();
			S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket,
					bucketKey));

			InputStream in = s3Object.getObjectContent();
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println(br.readLine());
			}
			s3Client.putObject(bucket, bucketKey, s3Object.getObjectContent(),
					s3Object.getObjectMetadata());

			Class.forName("org.postgresql.Driver");
			Connection connection = null;
			connection = DriverManager
					.getConnection(
							"jdbc:postgresql://testdb.cjotpija7r7c.us-east-1.rds.amazonaws.com:5432/dbname",
							"sumangal", "linuxlinux");
			if (connection != null)
				System.out.println("DB connection sucessful");
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "Ok";
	}
}
