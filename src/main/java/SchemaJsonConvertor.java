import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class SchemaJsonConvertor {

    private static final JsonConverter converterWithSchemaEnabled = new JsonConverter();

    protected static Struct createRecord(Schema schema, int i) {
        Struct struct = new Struct(schema);
        struct.put("ColA", "SchemaJsonConvertorCode"+i);
        struct.put("ColB", i);
        return struct;
    }

    protected static Schema createSchema() {
        return SchemaBuilder.struct().name("JsonSchema")
                .field("ColA", Schema.STRING_SCHEMA)
                .field("ColB", Schema.INT32_SCHEMA)
                .build();
    }


    public static void main(String[] args) {
        Map<String, String> config = new HashMap<>();
        String topicName = "SchemaJsonConvertorCode";
        config.put(JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG, "100");
        config.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        converterWithSchemaEnabled.configure(config);
        SchemaJsonConvertor producer = new SchemaJsonConvertor();
        Schema schema = createSchema();

        Properties props = producer.getProducerConfigs();
        KafkaProducer<String, byte[]> kafkaProducer = producer.getKafkaProducer(props);

        for (int i = 0; i < 10; i++) {
            Struct record = createRecord(schema,i);
            byte[] value = converterWithSchemaEnabled.fromConnectData(topicName, record.schema(), record);
            producerRecord(value, topicName, kafkaProducer);
        }
        closeProducer(kafkaProducer);


    }

    public Properties getProducerConfigs() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "id : " + UUID.randomUUID().toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return props;
    }

    public KafkaProducer<String, byte[]> getKafkaProducer(Properties properties) {
        return new KafkaProducer<>(properties);
    }

    public static void producerRecord(byte[] json, String kafkaTopicName, KafkaProducer<String, byte[]> kafkaProducer) {
        ProducerRecord record = new ProducerRecord<>(kafkaTopicName, null, json);
        kafkaProducer.send(record);
        kafkaProducer.flush();
    }

    public static void closeProducer(KafkaProducer<String, byte[]> kafkaProducer) {
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
