package com.gsoc.polina.kafka.poc.partitioner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class CountryPartitioner implements Partitioner {

	private static Map<String,Integer> countryToPartitionMap;

	// initialize partitions
	// !!!NOTE - in this method Kafka producer will pass all the properties 
	// that we've configured for the producer to the Partitioner class
	//  we read only those properties that start with partitions., parse them to get the partitionId
	public void configure(Map<String, ?> configs) {
		System.out.println("Inside CountryPartitioner.configure " + configs);
        countryToPartitionMap = new HashMap<String, Integer>();
        for(Map.Entry<String,?> entry: configs.entrySet()){
            if(entry.getKey().startsWith("partition.")){
                String keyName = entry.getKey();
                String value = (String)entry.getValue();
                int paritionId = Integer.parseInt(keyName.substring(10));
                countryToPartitionMap.put(value,paritionId);
            }
        }
		
	}

	// shut down the partitioner, any resource initialized has to be cleaned
	public void close() {
		//TODO 
	}

	// calls one for every message
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String countryName = ((String) value).split(":")[0];
        if(countryToPartitionMap.containsKey(countryName)){
            // if the country is mapped to particular partition return it
            return countryToPartitionMap.get(countryName);
        }else {
        	return new Random().nextInt(cluster.availablePartitionsForTopic(topic).size()) ;
        }
	}

}
