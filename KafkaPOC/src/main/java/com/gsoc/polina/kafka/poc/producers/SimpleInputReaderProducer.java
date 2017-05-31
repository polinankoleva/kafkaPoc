package com.gsoc.polina.kafka.poc.producers;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Simple Kafka producer that reads input from the console and put it as a
 * message into a specific topic. If a message with text "exit" is entered, the
 * producer will closed.
 * 
 * @author Polina Koleva
 *
 */
public class SimpleInputReaderProducer extends Thread {

	private String topic;
	private Properties props;

	public SimpleInputReaderProducer(String topic, String host, String keySerializer, String valueSerializer) {
		this.topic = topic;
		this.props = produceConfiguration(host, keySerializer, valueSerializer);
	}

	public void run() {
		Scanner in = new Scanner(System.in);
		System.out.println(
				"Enter your message(each new line is counted as a new message. If you want to exit, please enter exit as your message)");
		org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(this.props);
		String line = in.nextLine();
		while (!line.equals("exit")) {

			ProducerRecord<String, String> rec = new ProducerRecord<String, String>(this.topic, line);
			producer.send(rec);
			line = in.nextLine();
		}
		in.close();
		// do not forget to close the producer
		producer.close();
	}

	private Properties produceConfiguration(String host, String keySerializer, String valueSerializer) {
		// properties
		Properties configProperties = new Properties();
		// producers work only with bytes that's why we need serializers
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
		// key will be of type byte
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
		// value will be of type String
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
		return configProperties;
	}

}
