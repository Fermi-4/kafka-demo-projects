package state.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class JsonSerializer<T> implements Serializer<T> {

	private final ObjectMapper objectMapper = new ObjectMapper();


	public void close() {

	}

	public byte[] serialize(String topic, T data) {
		if (data == null)
			return null;

		try {
			return objectMapper.writeValueAsBytes(data);
		} catch (Exception e) {
			throw new SerializationException("Error serializing JSON message", e);
		}
	}

}
