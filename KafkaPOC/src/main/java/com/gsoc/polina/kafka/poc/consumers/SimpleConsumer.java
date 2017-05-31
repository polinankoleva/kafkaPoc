package com.gsoc.polina.kafka.poc.consumers;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

/**
 * A simple implementation of a Kafka Consumer. It subscribe to a specific topis,
 * reads its messages and outputs them into the console.
 * 
 * @author Polina Koleva
 *
 */
public class SimpleConsumer extends Thread {

	private String topicName;
	private String groupId;
	private KafkaConsumer<String, String> kafkaConsumer;
	private Properties props;

	public SimpleConsumer(String topicName, String groupId, String host, String keyDeseriazer,
			String valueDeserializer) {
		this.topicName = topicName;
		this.groupId = groupId;
		this.props = produceConfiguration(host, keyDeseriazer, valueDeserializer);
	}

	public void run() {
		this.kafkaConsumer = new KafkaConsumer<String, String>(this.props);
		// subscribe to a topic
		kafkaConsumer.subscribe(Arrays.asList(topicName));
		// Start processing messages
		try {
			while (true) {
				// poll every 100 milliseconds for new messages
				ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
				for (ConsumerRecord<String, String> record : records)
					System.out.println(record.value());
			}
		} catch (WakeupException ex) {
			System.out.println("Exception caught " + ex.getMessage());
		} finally {
			kafkaConsumer.close();
			System.out.println("After closing KafkaConsumer");
		}
	}

	public void stopConsumer() throws InterruptedException {
		this.kafkaConsumer.wakeup();
		System.out.println("Stopping consumer .....");
		this.join();
	}

	private Properties produceConfiguration(String host, String keySerializer, String valueSerializer) {
		Properties configProperties = new Properties();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerializer);
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerializer);
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
		return configProperties;
	}
}
