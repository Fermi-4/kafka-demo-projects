package state;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import state.serde.SlotDataSerde;

public class SlotDataNoStateExample {

	public static void main(String[] args) {
		
		StreamsBuilder builder = new StreamsBuilder();
		
		
		KStream<String, SlotData> slotDataStream = builder.stream("slot-data");
		
		slotDataStream.peek((k, v) ->{ 
				
				
				System.out.println( " val: " + v.toString());
			});
		
		Topology topology = builder.build();
		System.out.println(topology.toString());
		
		KafkaStreams stream = new KafkaStreams(topology, getAppConfig());
		
		Runtime.getRuntime().addShutdownHook(new Thread(()->stream.close()));
		
		stream.start();
	}
	
	
	private static Properties getAppConfig() {
		Properties p = new Properties();
		p.put(StreamsConfig.APPLICATION_ID_CONFIG, "slot-data-processor-no-state");
		p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SlotDataSerde.class);
//		p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0); // for testing only, not stable for production
		return p;
	}
	
}
