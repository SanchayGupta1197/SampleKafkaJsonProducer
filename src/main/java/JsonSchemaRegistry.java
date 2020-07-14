import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class JsonSchemaRegistry {


    protected static JsonNode getRecordObject(){
        ObjectMapper om = new ObjectMapper();
        String Value ="{\"ColA\":\"JsonSchemalessCode\",\"ColB\":1}";
        try {
            return om.readTree(Value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected static JsonNode getRecordSchema() {
        ObjectMapper om = new ObjectMapper();
        String jsonSchemaValue = "{\"type\":\"object\",\"properties\":{\"ColA\":{\"type\":\"string\"},\"ColB\":{\"type\":\"number\"}}}";
        try {
            return om.readTree(jsonSchemaValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {

        JsonSchemaRegistry producer = new JsonSchemaRegistry();
        JsonNode jsonSchema = getRecordSchema();
        JsonNode jsonValue = getRecordObject();
        ObjectNode value= JsonSchemaUtils.envelope(jsonSchema,jsonValue);
        Properties props = producer.getProducerConfigs();
        KafkaProducer<String, ObjectNode> kafkaProducer = producer.getKafkaProducer(props);
        for (int i = 0; i < 10; i++) {
            //writing the same record 10 times.
            producerRecord(value, "JsonSchemaConvertorCode", kafkaProducer);
        }
        closeProducer(kafkaProducer);

    }


    public Properties getProducerConfigs() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "id : " + UUID.randomUUID().toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");
        // props.put("value.schema", jsonSchema);
        return props;

    }

    public KafkaProducer<String, ObjectNode> getKafkaProducer(Properties properties) {
        return new KafkaProducer<>(properties);
    }

    public static void producerRecord(ObjectNode json, String kafkaTopicName, KafkaProducer<String, ObjectNode> kafkaProducer) {
        ProducerRecord record = new ProducerRecord<>(kafkaTopicName, null, json);
        kafkaProducer.send(record);
        kafkaProducer.flush();
    }

    public static void closeProducer(KafkaProducer<String, ObjectNode> kafkaProducer) {
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
