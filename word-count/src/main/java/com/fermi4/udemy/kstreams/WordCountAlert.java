package com.fermi4.udemy.kstreams;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WordCountAlert {
	static final long max = 5;
	public static void main(String[] args) {
		
		Properties config = getProperties();
		log.info("Creating builder...");
		StreamsBuilder builder = new StreamsBuilder();
		
		// 1) stream from kafka
		KStream<String, Long> wordCountInput = builder.stream("wc-output");
		
		
		
		wordCountInput

		.filter((key, value) -> (value >= max)).peek(
						(k, v) -> log.info("Rule violation: " + k + " has been moved " + v + " times and that is unacceptable!!!")
				).map(
						(k, v) -> KeyValue.pair(k, "Alert triggered for board: " + k + "; Moved " + v + "times")
						)
		.to("wc-alert", Produced.with(Serdes.String(), Serdes.String()));
		
		
		// start the alert
		KafkaStreams streams = new KafkaStreams(builder.build(), config);
		streams.start();

		// print topology
		log.info(streams.toString());

		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				log.info("Closing...");
				// clears the local state store - DO NOT DO IN PRODUCTION
				streams.cleanUp();
				streams.close();
			}

		});
		
	}
		
	
	
	
	private static Properties getProperties() {
		Properties p = new Properties();
		p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wc-alert");
		p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				"localhost:9092");
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
		p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
		return p;
	}
}
