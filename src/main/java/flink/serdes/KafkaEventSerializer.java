package flink.serdes;

import model.KafkaEvent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

public class KafkaEventSerializer implements KafkaRecordSerializationSchema<KafkaEvent> {

    public String topic;

    public KafkaEventSerializer(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaEvent element, KafkaSinkContext context, Long timestamp) {
        // key is the username
        byte[] key = element.getSenderName().getBytes(StandardCharsets.UTF_8);

        // value is the message
        String valueString = element.getMessage();
        byte[] value = valueString.getBytes(StandardCharsets.UTF_8);

        return new ProducerRecord<>(this.topic, key, value);
    }
}


