package state.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import state.BoardEvent;
import state.SlotData;
import state.serde.JsonDeserializer;
import state.serde.JsonSerializer;

/**
 * 
 * sends SlotData to Slot Data Topic
 * 
 * @author colte
 *
 */
public class SlotDataProducer {

	static final SlotDataSupplier _supplier = new SlotDataSupplier();
	private static final String SLOT_DATA_TOPIC = "slot-data";
	private static final long DELAY = 2000; // throttle
	static JsonSerializer<BoardEvent> jsonBoardEventSerializer = new JsonSerializer<BoardEvent>();
	static JsonDeserializer<BoardEvent> jsonBoardEventDeserializer = new JsonDeserializer<BoardEvent>(BoardEvent.class);
	static JsonSerializer<SlotData> jsonSlotDataSerializer = new JsonSerializer<SlotData>();
	static JsonDeserializer<SlotData> jsonSlotDataDeserializer = new JsonDeserializer<SlotData>(SlotData.class);
	static KafkaProducer<String, SlotData> producer = new KafkaProducer<String, SlotData>(getProducerConfig());
	
	public static void main(String[] args) throws InterruptedException {
		
		
		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				producer.close();
			}
			
		});
		
		while(true) {
			SlotData slotdata = _supplier.get();
			System.out.println("Sending record: " + slotdata.toString());
			producer.send(new ProducerRecord<String, SlotData>(SLOT_DATA_TOPIC, slotdata));
			Thread.sleep(DELAY);
		}
		

	}

	private static Map<String, Object> getProducerConfig() {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		map.put(ProducerConfig.CLIENT_ID_CONFIG, "slot-data-producer");
		map.put(ProducerConfig.RETRIES_CONFIG, 10);  
		map.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
		map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, jsonSlotDataSerializer.getClass());
		return map;
	}

	
}
