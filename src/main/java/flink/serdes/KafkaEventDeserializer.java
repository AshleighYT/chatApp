package flink.serdes;

import model.KafkaEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class KafkaEventDeserializer implements KafkaRecordDeserializationSchema<KafkaEvent> {

    public String topic;

    public KafkaEventDeserializer() {}

    @Override
    public TypeInformation<KafkaEvent> getProducedType() {

        return TypeInformation.of(KafkaEvent.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaEvent> out) throws IOException {
        long timestamp = record.timestamp();

        String username = record.key() != null ? new String(record.key(), StandardCharsets.UTF_8) : "defaultUsername";
        String fullMessage = record.value() != null ? new String(record.value(), StandardCharsets.UTF_8) : "";
        String[] parts = fullMessage.split(":", 3);
        if (parts.length < 3) {
            return;
        }
        String receiver = parts[0];
        String sender = parts[1];

        out.collect(new KafkaEvent(sender, receiver, fullMessage, timestamp));
    }
}