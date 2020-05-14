package state.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import lombok.NoArgsConstructor;
import state.SlotData;

/**
 * 
 * Have to create a custom serde as the factory method doesn't 
 * provide an empty default constructor which is needed as they get 
 * registered for reflection
 * 
 * @author colte
 *
 */
@NoArgsConstructor
public class SlotDataSerde implements Serde<SlotData> {
	
	private final JsonDeserializer<SlotData> jsonDeserializer = new JsonDeserializer<SlotData>(SlotData.class);
	private final JsonSerializer<SlotData> jsonSerializer = new JsonSerializer<SlotData>();
	
	@Override
	public Serializer<SlotData> serializer() {
		return jsonSerializer;
	}

	@Override
	public Deserializer<SlotData> deserializer() {
		return jsonDeserializer;
	}

}
