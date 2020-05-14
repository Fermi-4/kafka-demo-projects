package state.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonDeserializer<T> implements Deserializer<T>{
	

	private final ObjectMapper objectMapper = new ObjectMapper();
	
	Class<T> clazz;

	

	public JsonDeserializer(Class<T> clazz) {
		super();
		this.clazz = clazz;
	}



	public T deserialize(String topic, byte[] data) {
		  if (data == null)
	            return null;

	        T t;
	        try {
	            t = objectMapper.readValue(data, clazz);
	        } catch (Exception e) {
	            throw new SerializationException(e);
	        }

	        return t;
	}

}
