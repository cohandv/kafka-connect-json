package converters;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer implements Deserializer<JsonNode> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonDeserializer() {
        this(Collections.emptySet(), JsonNodeFactory.withExactBigDecimals(true));
    }

    /**
     * A constructor that additionally specifies some {@link DeserializationFeature}
     * for the deserializer
     *
     * @param deserializationFeatures the specified deserialization features
     * @param jsonNodeFactory the json node factory to use.
     */
    JsonDeserializer(
            final Set<DeserializationFeature> deserializationFeatures,
            final JsonNodeFactory jsonNodeFactory
    ) {
        deserializationFeatures.forEach(objectMapper::enable);
        objectMapper.setNodeFactory(jsonNodeFactory);
    }

    @Override
    public JsonNode deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        JsonNode data;
        try {
            data = objectMapper.readTree(bytes);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }
}
