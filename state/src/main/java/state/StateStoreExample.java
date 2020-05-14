package state;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import state.serde.JsonDeserializer;
import state.serde.JsonSerializer;
import state.serde.SlotDataSerde;

public class StateStoreExample {
	static final String SLOT_DATA_TOPIC = "slot-data";
	static final String BOARD_EVENT_TOPIC = "board-churn-event";
	static final String STATE_STORE = "slot-data-state";
	
	static JsonSerializer<BoardEvent> jsonBoardEventSerializer = new JsonSerializer<BoardEvent>();
	static JsonDeserializer<BoardEvent> jsonBoardEventDeserializer = new JsonDeserializer<BoardEvent>(BoardEvent.class);
	static JsonSerializer<SlotData> jsonSlotDataSerializer = new JsonSerializer<SlotData>();
	static JsonDeserializer<SlotData> jsonSlotDataDeserializer = new JsonDeserializer<SlotData>(SlotData.class);


	public static void main(String[] args) throws InterruptedException {
			/*
			 * the state store
			 */
			StoreBuilder<KeyValueStore<String, SlotData>> stateStore = Stores
					.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(STATE_STORE), Serdes.String(),
							Serdes.serdeFrom(jsonSlotDataSerializer, jsonSlotDataDeserializer));
//					.withLoggingEnabled(getStateStoreConfig());

			/*
			 * get the streams builder instance to construct our topology
			 */
			StreamsBuilder builder = new StreamsBuilder();
			
			/*
			 * register the state store
			 */
			builder.addStateStore(stateStore);
			
			/*
			 * read from slot data topic
			 */
			KStream<String, SlotData> slotDataUpdateStream = builder.stream(SLOT_DATA_TOPIC, Consumed.with(Serdes.String(), Serdes.serdeFrom(jsonSlotDataSerializer, jsonSlotDataDeserializer)));
			
			slotDataUpdateStream.transformValues(new ValueTransformerSupplier<SlotData, BoardEvent>() {

				public ValueTransformer<SlotData, BoardEvent> get() {
					return new SlotDataEventTransformer();
				}
			
			}, STATE_STORE)
			
			// filter out null/no event
			.filter((k, v) -> (v != null))
			
			// rekey the data
			.selectKey((k, v) -> v.getBoardId())
			
			// log to the console so we can see
			.peek((k, v) -> System.out.println("\nBoardEvent: \nBoard: " + v.getBoardId() + "\nType: " + v.getType()))
			
			// publish to events topic
			.to(BOARD_EVENT_TOPIC, Produced.with(Serdes.String(), Serdes.serdeFrom(jsonBoardEventSerializer, jsonBoardEventDeserializer)));
			
			
			
			
			/*
			 * construct the KafkaStream App
			 */
			Topology topology = builder.build();
			System.out.println(topology.describe());
			KafkaStreams stream = new KafkaStreams(topology, getAppConfig());
			
			/*
			 * notify when state changes
			 */
			stream.setStateListener((s, t) -> System.out.println("state updated: " + t.toString() + " --> " + s.toString())); 
			
			CountDownLatch latch = new CountDownLatch(1);
		
			/*
			 * add shutdown hook
			 */
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					stream.cleanUp();
					stream.close();
					latch.countDown();
				}
			});
			
			/*
			 * start the application
			 */
			try{
	            stream.start();
	            latch.await();
	        } catch (Throwable e){
	            System.exit(1);
	        }
	        System.exit(0);
	}
	

	private static Properties getAppConfig() {
		Properties p = new Properties();

		p.put(StreamsConfig.APPLICATION_ID_CONFIG, "slot-data-processor-with-state");
		p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SlotDataSerde.class);
//		p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0); // for testing only, not stable for production
		return p;
	}

	@SuppressWarnings("unused")
	private static Map<String, String> getStateStoreConfig() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("retention.ms", "172800000");
		map.put("retention.bytes", "10000000000");
		map.put("cleanup.policy", "delete");
		return map;
	}
}