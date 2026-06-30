package ma.enset.exercise2.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

/**
 * Generic Serde (Serializer/Deserializer) implementation using Jackson ObjectMapper.
 * This class translates Java objects to JSON byte arrays and vice versa.
 */
public class JsonSerde<T> implements Serde<T>, Serializer<T>, Deserializer<T> {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration required
    }

    @Override
    public void close() {
        // No resources to close
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing to JSON for type: " + type.getName(), e);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            return mapper.readValue(data, type);
        } catch (IOException e) {
            throw new RuntimeException("Error deserializing from JSON for type: " + type.getName(), e);
        }
    }
}
