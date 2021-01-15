package com.kinesis.consumer.test.kinesisdatastreamconsumer;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class sampleConsumer{

    static private String STREAM_NAME = "milk-data-stream";
    public static void main(String args[]) {

        BasicAWSCredentials awsCreds = new BasicAWSCredentials("aws_access_key_id",
                "aws_secret_access_key");
        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        clientBuilder.setRegion("us-east-1"); // 
        clientBuilder.setCredentials(new AWSStaticCredentialsProvider(awsCreds));
        AmazonKinesis kinesisClient = clientBuilder.build();

        String shardIterator ="milk-consumer";
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(STREAM_NAME);
        getShardIteratorRequest.setShardId("shardId-000000000000");
        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
        GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
        shardIterator = getShardIteratorResult.getShardIterator();

        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        while(true)
        {
            try{
                getRecordsRequest.setLimit(100);
                getRecordsRequest.setShardIterator(shardIterator);
                GetRecordsResult result = kinesisClient.getRecords(getRecordsRequest);
                List<Record> records = result.getRecords();
                for (Record r : records) {
                    System.out.println(r.getSequenceNumber());
                    System.out.println(r.getPartitionKey());
                    byte[] bytes = r.getData().array();
                    System.out.println(new String(bytes));
                }
                Thread.sleep(1);
            }catch(Exception e)
            {
                System.out.println(e.toString());
            }   
        }
  
        
    }
}