package converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class JsonSerializer  implements Serializer<JsonNode> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonSerializer() {
        this(Collections.emptySet(), JsonNodeFactory.withExactBigDecimals(true));
    }

    /**
     * A constructor that additionally specifies some {@link SerializationFeature}
     * for the serializer
     *
     * @param serializationFeatures the specified serialization features
     * @param jsonNodeFactory the json node factory to use.
     */
    JsonSerializer(
            final Set<SerializationFeature> serializationFeatures,
            final JsonNodeFactory jsonNodeFactory
    ) {
        serializationFeatures.forEach(objectMapper::enable);
        objectMapper.setNodeFactory(jsonNodeFactory);
    }

    @Override
    public byte[] serialize(String topic, JsonNode data) {
        if (data == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }
}
