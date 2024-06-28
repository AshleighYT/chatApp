package chatapp;

import chatapp.utils.KafkaConfiguration;
import flink.process.SensitiveWordFilter;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {
    private final String topic;
    private final String username;
    private final KafkaReceiver<Object, Object> receiver;

    private static final Logger LOG = LoggerFactory.getLogger(SensitiveWordFilter.class);


    public Consumer(String username, String bootstrapServers, String groupId, String topic) {
        this.username = username;
        this.topic = topic;
        Properties props = KafkaConfiguration.consumerProps(groupId);

        ReceiverOptions<Object, Object> receiverOptions = ReceiverOptions.create(props)
                .subscription(Collections.singleton(topic));
        this.receiver = KafkaReceiver.create(receiverOptions);
    }

    public void receiveMessages() {
        receiver.receive()
                .doOnNext(record -> {
                            String topic = record.topic();
                            if ("warning-data".equals(topic)) {
                                System.out.println(record.value());
                            } else {
                                String value = (String) record.value();
                                String[] parts = value.split(":", 3);
                                if (parts.length < 3) {
                                    return;
                                }
                                String receiver = parts[0];
                                String sender = parts[1];
                                String message = parts[2];;
                                if (!this.username.equals(receiver)) {
                                    return;
                                }

                                // Extract timestamp
                                long timestamp = record.timestamp();
                                ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("Asia/Shanghai"));
                                String formattedTimestamp = zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                                // Concatenate sender, message, and timestamp
                                String output = String.format("(%s) %s [%s]", sender, message, formattedTimestamp);

                                System.out.println(output);

                                record.receiverOffset().acknowledge();}
                })
                .subscribe();
    }

}
