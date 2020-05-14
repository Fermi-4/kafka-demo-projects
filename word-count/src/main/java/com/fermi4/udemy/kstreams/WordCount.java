package com.fermi4.udemy.kstreams;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.suppress.StrictBufferConfigImpl;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WordCount {

	public static void main(String[] args) {
		log.info("Starting WordCount KStreams Application...");

		Properties config = getProperties();
		StreamsBuilder builder = new StreamsBuilder();

		
		
		
		// 1) stream from kafka
		KStream<String, String> wordCountInput = builder.stream("wc-input");

		// 2) map all to lowercase
		KTable<Windowed<String>, Long> kTableCounts = wordCountInput.mapValues(value -> value.toLowerCase())

				// 3) flat map values, split by space " "
				.flatMapValues(value -> Arrays.asList(value.split(" ")))

				// 4) rekey - select a new key
				.selectKey((ignoreKey, word) -> word)

				// 5) Group by keys before the aggregation
				.groupByKey()

				// window
				.windowedBy(TimeWindows.of(Duration.ofSeconds(10)).grace(Duration.ZERO))

				// 6) count words
				.count(Materialized.with(Serdes.String(), Serdes.Long()))

				.suppress(Suppressed.untilWindowCloses(new StrictBufferConfigImpl().withNoBound()));

		
		
		
		
		
		kTableCounts.toStream().map((k, v) -> KeyValue.pair(k.key(), v))
				.peek((k, v) -> System.out.println("Key: " + k.toString() + " value: " + v.toString())).to("wc-output", Produced.with(Serdes.String(), Serdes.Long()));

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
		p.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
		p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
		return p;
	}

}
