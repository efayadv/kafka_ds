package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;
import com.simplekafka.broker.Protocol.PartitionMetadata;
import com.simplekafka.broker.Protocol.TopicMetadata;

import java.io.IOException;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
public class SimpleKafkaClient {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaClient.class.getName());
    private static final int DEFAULT_BUFFER_SIZE = 4096;
    
    private final String bootstrapBroker;
    private final int bootstrapPort;
    private final Map<String, TopicMetadata> topicMetadata;
    private final Map<Integer, BrokerInfo> brokers;
    private final AtomicInteger correlationId;


    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
        this.topicMetadata = new ConcurrentHashMap<>();
        this.brokers = new ConcurrentHashMap<>();
        this.correlationId = new AtomicInteger(0);
    }
    public void initialize() throws IOException {
        refreshMetadata();
    }
    public void refreshMetadata() throws IOException {
        // Fetch cluster metadata from broker
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(bootstrapBroker, bootstrapPort));

            //request metadata
            ByteBuffer request = Protocol.encodeMetadataRequest();
            channel.write(request);

            //Read response
            ByteBuffer response = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            response.flip();
            Protocol.MetadataResult result = Protocol.decodeMetadataResponse(response);

            if (!result.isSuccess()) {
                throw new IOException("Failed to fetch metadata: " + result.getError());
            }

            brokers.clear();    

            //extract ids from brokers
            
            for (BrokerInfo broker : result.getBrokers()) {
                brokers.put(broker.getId(), new BrokerInfo(broker.getId(), broker.getHost(), broker.getPort()));
            }
            
            //update topic metadata, conchashmap <String, Topic Metadata>
            topicMetadata.clear();
            for (Protocol.TopicMetadata topic : result.getTopics()) {
                List<PartitionMetadata> partitions = new ArrayList<>()

                topicMetadata.put(topic.getName(), topic);
            }
            


        } catch () {

        }

    }
    public long send(String topic, int partition, byte[] message) throws IOException {
        // Send a message to a specific topic-partition
        return -1; // Placeholder
    }
    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        // Fetch messages from a topic-partition
        return new ArrayList<>(); // Placeholder
    }
    // Additional methods and inner classes for metadata
}