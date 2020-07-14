import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class StringConvertor {

    public static void main(String[] args) throws IOException {
        StringConvertor producer = new StringConvertor();
        Properties props = producer.getProducerConfigs();
        KafkaProducer<String, String> kafkaProducer = producer.getKafkaProducer(props);
        String value;
        for (int i = 0; i < 10; i++) {
            //writing the same record 10 times.
            value= String.format("{\"ColA\":\"JsonStringCode%s\",\"ColB\":%s}", i, i);
            producerRecord(value, "JsonConvertorStringCode", kafkaProducer);
        }
        closeProducer(kafkaProducer);

    }


    public Properties getProducerConfigs() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "id : " + UUID.randomUUID().toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;

    }

    public KafkaProducer<String, String> getKafkaProducer(Properties properties) {
        return new KafkaProducer<>(properties);
    }

    public static void producerRecord(String json, String kafkaTopicName, KafkaProducer<String, String> kafkaProducer) {
        ProducerRecord record = new ProducerRecord<>(kafkaTopicName, null, json);
        kafkaProducer.send(record);
        kafkaProducer.flush();
    }

    public static void closeProducer(KafkaProducer<String, String> kafkaProducer) {
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}



