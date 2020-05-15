package runner;

import org.apache.kafka.common.serialization.Serdes;

import runner.runner.KafkaRunner;
import runner.runner.KafkaRunnerImpl;

public class Main {
	/*
	 * KafkaRunnerImpl(Serde<K> _keySerde, Serde<T> _valueSerde, String _bootstrapServers, String _topic, long _requestTimeout,
			long _sampleTimeout)
	 */
	static KafkaRunner<String> runner = new KafkaRunnerImpl<String, String>(Serdes.String().deserializer(), Serdes.String().deserializer(), "localhost:9092", "test-topic-multipart-0", 1000, 1000);
	
	public static void main(String[] args) {
		runner.pull().stream().forEach($ -> System.out.println($));
	}
	
}
