package runner.runner;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * This class pulls an entire Kafka topic and then exits based on the latest
 * offsets in the topic
 * 
 * @author Fermi-4
 *
 * @param <K>
 * @param <T>
 */
@Slf4j
public class KafkaRunnerImpl<K, T> implements KafkaRunner<T> {

	final Deserializer<K> _keySerde;
	final Deserializer<T> _valueSerde;
	final String _bootstrapServers;
	final String _topic;
	final int _requestTimeout;
	final int _sampleTimeout;
	
	public KafkaRunnerImpl(Deserializer<K> _keySerde, Deserializer<T> _valueSerde, String _bootstrapServers, String _topic, int _requestTimeout,
			int _sampleTimeout) {
		super();
		this._topic = _topic;
		this._keySerde = _keySerde;
		this._valueSerde = _valueSerde;
		this._bootstrapServers = _bootstrapServers;
		this._requestTimeout = _requestTimeout;
		this._sampleTimeout = _sampleTimeout;
	}
	
	private Map<TopicPartition, Long> _endOffsets;
	
	/**
	 * 
	 * https://stackoverflow.com/questions/52837302/strategies-to-ensure-that-each-kafka-consumer-has-a-different-unique-group-id
	 * 
	 * in order to ensure we get all of the data from the partitions of a topic we need 
	 * to manually assign the partitions to the consumer.
	 * 
	 * There are tradeoffs to doing this and is not the intended use for kafka (see above)
	 * 
	 * 
	 */
	public Collection<T> pull() {
		try(KafkaConsumer<K, T> consumer = new KafkaConsumer<K, T>(getConsumerProps())) {
		
			Collection<T> records = new ArrayList<T>();
		
			/*
			 * subscribe to topic
			 */
			
			List<PartitionInfo> info = consumer.partitionsFor(_topic);
			
			/*
			 * manually assign the partitions
			 */
			 consumer.assign(info.stream().map(partitionInfo -> new TopicPartition(_topic, partitionInfo.partition())).collect(Collectors.toList()));
			
			/*
			 * partitions belonging to this consumer
			 */
			Set<TopicPartition> partitions = consumer.assignment();
			
			/*
			 * seek the beginning of assigned partitions
			 */
//			consumer.seekToBeginning(partitions);
			
			/*
			 * the target partition end offsets
			 */
			_endOffsets = consumer.endOffsets(partitions);
			log.info(_endOffsets.toString());
			
			/*
			 * flag once all end offsets have been met - exit loop
			 */
			boolean offsetsNotReachedForAllPartitions = true;
			
			/*
			 * start loop
			 */
			while(offsetsNotReachedForAllPartitions) {
				/*
				 * poll until sampleTimeout is reached
				 */
				ConsumerRecords<K, T> sample = consumer.poll(Duration.ofMillis(_sampleTimeout));		
				
				for(TopicPartition partition : partitions) {
					/*
					 * grab the records only from this partition
					 */
					List<ConsumerRecord<K, T>> recordsForPartition = sample.records(partition);
					
					/*
					 * iterate through all the records and add to the collection 
					 * ONLY if the record offset is less than the target offset
					 * for current partition
					 */
					for(ConsumerRecord<K, T> consumerRecord : recordsForPartition) {
						log.info("record offset: " + consumerRecord.offset() + " partition: " + consumerRecord.partition());
						if(consumerRecord.offset() <= _endOffsets.get(partition)) {
							records.add(consumerRecord.value());
						}
					}
				}
				
	
				offsetsNotReachedForAllPartitions = comparePartitionOffsets(consumer);
			}
		
			return records;
		
		} catch (Exception e) {
			// TODO: how to handle the exceptions?
			e.printStackTrace();
		}
		
		return null;
	}

	private boolean comparePartitionOffsets(KafkaConsumer<K, T> consumer) {
		return !_endOffsets.keySet().stream().allMatch(tp -> {
			
//			log.info("consumer: " +consumer.position(tp) + " target: " + _endOffsets.get(tp));
			boolean offsetsReached = consumer.position(tp) >= (_endOffsets.get(tp)-1);
			if(offsetsReached) log.info("Target offsets reached for partition: " + tp.partition());
			return offsetsReached;
		});
	}

	private Properties getConsumerProps() {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "runner-group");
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, _requestTimeout); // polls until this timeout is reached then closes
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, _valueSerde.getClass());
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, _keySerde.getClass());
		return properties;
	}



}
