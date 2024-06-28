package chatapp;

import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import java.util.Properties;
import java.util.Scanner;
import chatapp.utils.KafkaConfiguration;

public class Producer {
    private final KafkaSender<String, String> sender;
    private final String username;
    private final String topic;

    public Producer(String username, Properties props) {
        this.username = username;
        this.topic = "chat-data";
        SenderOptions<String, String> senderOptions = SenderOptions.create(props);
        this.sender = KafkaSender.create(senderOptions);
    }

    public void sendMessage() {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Enter your message. Type 'exit' to quit:");
            while (true) {
                String message = scanner.nextLine();
                if ("exit".equalsIgnoreCase(message.trim())) {
                    break;
                }
                String[] parts = message.split(":", 2);
                if (parts.length < 2) {
                    System.out.println("Invalid message format. Please use the format 'receiver:message'");
                    continue;
                }
                String receiver = parts[0].trim();
                String messageContent = parts[1].trim();

                // Message format: "receiver:sender:messageContent"
                String messageToSend = receiver + ":" + this.username + ":" + messageContent;

                sender.send(Mono.just(SenderRecord.create(new ProducerRecord<>(topic, username, messageToSend), null)))
                        .doOnError(e -> System.out.println("Send failed: " + e.getMessage()))
                        .subscribe();
            }
        } finally {
            sender.close();
        }
    }

//    public static void main(String[] args) {
//        String username = args[0];
//        String clientId = "producer-" + username;
//        Properties props = KafkaConfiguration.producerProps(clientId);
//        Producer producer = new Producer(username, props);
//        producer.sendMessage();
//    }
}
