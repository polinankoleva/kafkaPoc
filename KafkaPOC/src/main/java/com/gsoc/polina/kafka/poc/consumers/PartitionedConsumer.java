package com.gsoc.polina.kafka.poc.consumers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

public class PartitionedConsumer extends Thread {

	private String topicName;
	private String groupId;
	private KafkaConsumer<String, String> kafkaConsumer;
	private Properties props;

	public PartitionedConsumer(String topicName, String groupId, String host, String keyDeseriazer,
			String valueDeserializer) {
		this.topicName = topicName;
		this.groupId = groupId;
		this.props = produceConfiguration(host, keyDeseriazer, valueDeserializer);
	}

	public void run() {
		this.kafkaConsumer = new KafkaConsumer<String, String>(this.props);
		// subscribe to a topic
		kafkaConsumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				System.out.printf("%s topic-partitions are revoked from this consumer\n",
						Arrays.toString(partitions.toArray()));
			}

			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				System.out.printf("%s topic-partitions are assigned to this consumer\n",
						Arrays.toString(partitions.toArray()));
			}
		});
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
