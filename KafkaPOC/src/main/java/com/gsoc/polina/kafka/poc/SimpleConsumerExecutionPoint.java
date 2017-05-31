package com.gsoc.polina.kafka.poc;

import com.gsoc.polina.kafka.poc.consumers.SimpleConsumer;

public class SimpleConsumerExecutionPoint {
	
	public static String SERVER_HOST = "localhost:9092";
	public static String TOPIC_NAME = "test";
	public static String STRING_DESERIALIZER_CLASS_NAME = "org.apache.kafka.common.serialization.StringDeserializer";
	
	public static void main(String[] args) {
		// start consumer 
		SimpleConsumer consumer = new SimpleConsumer(TOPIC_NAME, "group1", SERVER_HOST, STRING_DESERIALIZER_CLASS_NAME, STRING_DESERIALIZER_CLASS_NAME);
		consumer.start();
		
	}
}
