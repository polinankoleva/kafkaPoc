package com.gsoc.polina.kafka.poc.producers;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.gsoc.polina.kafka.poc.partitioner.CountryPartitioner;

/**
 * Simple Kafka producer that reads input from the console and put it as a
 * message into a specific topic. If a message with text "exit" is entered, the
 * producer will closed. Partitioning is done by country and a custom partition
 * class is specified (see {@link CountryPartitioner}). Two partitions are added
 * - one for country "Germany" and one for "Bulgaria". You can produce a message
 * to a specific partition by using key "Germany" or "Bulgaria".
 * 
 * @author Polina Koleva
 *
 */
public class PartitionedProducer extends Thread {

	private String topic;
	private Properties props;

	public PartitionedProducer(String topic, String host, String keySerializer, String valueSerializer) {
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
			ProducerRecord<String, String> rec = new ProducerRecord<String, String>(this.topic, null, line);
			producer.send(rec, new Callback() {
				// print some meta data about to which partiton the message is
				// sent
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					System.out.println("Message sent to topic ->" + metadata.topic() + " , parition->"
							+ metadata.partition() + " stored at offset->" + metadata.offset());
					;
				}
			});
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
		configProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CountryPartitioner.class.getCanonicalName());
		// add partitions - TODO add them dymanically
		configProperties.put("partition.1", "Germany");
		configProperties.put("partition.2", "Bulgaria");
		return configProperties;
	}
}
