package chatapp;

import java.util.Scanner;
import java.util.Properties;
import chatapp.utils.KafkaConfiguration;

public class ChatMain {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Open a send or receive window? [sender/receiver]");
        String role = scanner.nextLine().trim();

        System.out.println("Enter username:");
        String username = scanner.nextLine().trim();

        String topic_processed_data = "processed-data";
        String topic_warning = "warning-data";
        String bootstrapServers = "localhost:9092,localhost:9093,localhost:9094";


        switch (role.toLowerCase()) {
            case "sender":
                startProducer(username);
                String groupId_sender = "warning-group-" + username;
                startConsumer(username, bootstrapServers, groupId_sender, topic_warning);
                break;
            case "receiver":
                String groupId_receiver = "chatapp-group-" + username;
                startConsumer(username, bootstrapServers, groupId_receiver, topic_processed_data);
                break;
            default:
                System.out.println("Invalid role selected. Please restart the application and choose either 'sender' or 'receiver'.");
                break;
        }
    }

    private static void startProducer(String username) {
        Properties producerProps = KafkaConfiguration.producerProps(username);
        Producer producer = new Producer(username, producerProps);
        Thread producerThread = new Thread(producer::sendMessage);
        producerThread.start();
    }

    private static void startConsumer(String username, String bootstrapServers, String groupId, String topic) {
        Consumer consumer = new Consumer(username, bootstrapServers, groupId, topic);
        Thread consumerThread = new Thread(consumer::receiveMessages);
        consumerThread.start();
    }
}


