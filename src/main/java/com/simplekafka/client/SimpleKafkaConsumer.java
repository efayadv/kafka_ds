package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
public class SimpleKafkaConsumer {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());
    private static final int MAX_BYTES = 1024 * 1024; // 1MB max fetch size
    private static final int POLL_INTERVAL_MS = 100;

    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;
    private long currentOffset;
    private final AtomicBoolean running;
    private Thread consumerThread;

    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition) {
        this(bootstrapBroker, bootstrapPort, topic, partition, 0);
    }

    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition, long startOffset) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = 0;
        this.running = new AtomicBoolean(false);
    }

    public void initialize() throws IOException {
        client.initialize();

        if (client.getTopicMetadata(topic) == null) {
            throw new IOException("Topic does not exist: " + topic);
        }
    }

    public void seek(long offset) {
        this.currentOffset = offset;
    }

    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic, partition, currentOffset, 1024 * 1024);
        if (!messages.isEmpty()) {
            currentOffset += messages.size();
        }
        return messages;
    }

    public void startConsuming(MessageHandler handler) {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                try {
                    while (running.get()) {
                        List<byte[]> messages = poll();

                        for (byte[] message : messages) {
                            handler.handle(message, currentOffset - messages.size() + messages.indexOf(message));
                        }
                        // If no messages, wait a bit before polling again
                        if (messages.isEmpty()) {
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        LOGGER.log(Level.SEVERE, "Error in consumer loop", e);
                    }
                    running.set(false);
                }
            });
            consumerThread.setDaemon(true);
            consumerThread.start();

            LOGGER.info("Started consuming from topic: " + topic + ", partition: " + partition);
        }
    }

    public void stopConsuming() {
        if (running.compareAndSet(true, false)) {
            if (consumerThread != null) {
                try {
                    consumerThread.interrupt();
                    consumerThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            LOGGER.info("Stopped consuming from topic: " + topic + ", partition: " + partition);
        }
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public boolean isRunning() {
        return running.get();
    }

    public void close() {
        stopConsuming();
    }

    public interface MessageHandler {
        void handle(byte[] message, long offset);
    }


    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: SimpleKafkaConsumer <broker> <port> <topic> <partition>");
            System.exit(1);
        }
        
        String broker = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        int partition = Integer.parseInt(args[3]);
        
        try {
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(broker, port, topic, partition);
            consumer.initialize();
            
            System.out.println("Consumer initialized. Starting consumption...");
            
            // Consume messages and print them
            consumer.startConsuming((message, offset) -> {
                String messageStr = new String(message, StandardCharsets.UTF_8);
                System.out.println("Received message at offset " + offset + ": " + messageStr);
            });
            
            System.out.println("Consumer started. Press enter to stop.");
            System.in.read();
            
            consumer.close();
            System.out.println("Consumer stopped");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Consumer error", e);
        }
    }

}