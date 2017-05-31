package com.gsoc.polina.kafka.poc;

import com.gsoc.polina.kafka.poc.producers.SimpleInputReaderProducer;

public class SimpleProducerMainExecutionPoint {

	public static String SERVER_HOST = "localhost:9092";
	public static String TOPIC_NAME = "test";
	public static String BYTE_ARRAY_SERIALIZER_CLASS_NAME = "org.apache.kafka.common.serialization.ByteArraySerializer";
	public static String STRING_SERIALIZER_CLASS_NAME = "org.apache.kafka.common.serialization.StringSerializer";
	
	public static void main(String[] args) {
		// start procuder 
		SimpleInputReaderProducer simpleProducer = new SimpleInputReaderProducer(TOPIC_NAME, SERVER_HOST,
				BYTE_ARRAY_SERIALIZER_CLASS_NAME, STRING_SERIALIZER_CLASS_NAME);
		simpleProducer.start();	
	}

}
