package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
public class SimpleKafkaConsumer {
    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;
    private long currentOffset;
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = 0;
    }
    public void initialize() throws IOException {
        client.initialize();
    }
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic, partition, currentOffset, 1024 * 1024);
        if (!messages.isEmpty()) {
            currentOffset += messages.size();
        }
        return messages;
    }
    // Additional methods
}