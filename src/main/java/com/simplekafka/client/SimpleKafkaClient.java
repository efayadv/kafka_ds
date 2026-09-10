package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;
import com.simplekafka.broker.SimpleKafkaBroker;
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
                List<PartitionInfo> partitions = new ArrayList<>();

                for (Protocol.PartitionMetadata partition : topic.getPartitions()) {
                    PartitionInfo p = new PartitionInfo(partition.getId(), partition.getLeader(), partition.getReplicas());
                    partitions.add(p);
                }

                topicMetadata.put(topic.getName(), new TopicMetadata(topic.getName(), partitions));
            }

            LOGGER.info("Metadata refreshed: " + brokers.size() + " brokers, " + 
                       topicMetadata.size() + " topics");
        } 
    }

    public boolean createTopic(String topic, int numPartitions, short replicationFactor) throws IOException {
        if (brokers.isEmpty()) {
            refreshMetadata();
            if (brokers.isEmpty()) {
                throw new IOException("No brokers available");
            }
        }

        BrokerInfo broker = brokers.values().iterator().next();

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));

            // send create topic request
            ByteBuffer request = Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor);
            channel.write(request);

            //read response
            ByteBuffer response = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = channel.read(response);
            if (bytesRead < 0) {
                throw new IOException("No data from broker");
            }

            response.flip();
            
            byte responseType = response.get();
            if (responseType != Protocol.CREATE_TOPIC_RESPONSE){
                if (responseType == Protocol.ERROR_RESPONSE) {
                    short errorLength = response.getShort();
                    byte[] errorBytes = new byte[errorLength];
                    response.get(errorBytes);
                    String error = new String(errorBytes);
                    LOGGER.warning("Error creating topic: " + topic);
                    return false;
                }
                throw new IOException("Invalid create topic response type: " + responseType);
            }

            byte status = response.get();
            boolean success = status == 0;

            if (success) {
                //refresh metadata to include new topic
                refreshMetadata();
            }

            return success; 
        }  
    }

    public long send(String topic, int partition, byte[] message) throws IOException {
        // Send a message to a specific topic-partition
        if (!topicMetadata.containsKey(topic)) {
            refreshMetadata();
            if (!topicMetadata.containsKey(topic)) {
                //
                throw new IOException("Topic not found: " + topic);
            }
        }

        //find leader of partition
        //break down from topicMetadata -> PartitionMetdata -> getLeader()
        TopicMetadata metadata = topicMetadata.get(topic);
        List<PartitionInfo> partitions = metadata.getPartitions();
        PartitionInfo pInfo = null;

        for (PartitionInfo info : partitions) {
            if (info.getId() == partition) {
                pInfo = info;
                break;
            }
        }

        if (pInfo == null) {
            throw new IOException("partition not available: " + partition);
        }
    
        int leader = pInfo.getLeader();
        BrokerInfo brokerLeader = brokers.get(leader);

        if (brokerLeader == null) {
            refreshMetadata();
            brokerLeader = brokers.get(leader);

            if (brokerLeader == null) {
                throw new IOException("Leader broker not found");
            }
        }

        //send to leader
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(brokerLeader.getHost(), brokerLeader.getPort()));

            ByteBuffer request = Protocol.encodeProduceRequest(topic, partition, message);
            channel.write(request);

            ByteBuffer response = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received");
            }

            response.flip();
            Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);

            if (result.isSuccess()) {
                throw new IOException("Failed to produce message: " + result.getError());
            }

            return result.getOffset();
        }

    }
    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        // Fetch messages from a topic-partition
        return new ArrayList<>(); // Placeholder
    }
    // Additional methods and inner classes for metadata

    public static class TopicMetadata {
        private final String name;
        private final List<PartitionInfo> partitions;
        
        public TopicMetadata(String name, List<PartitionInfo> partitions) {
            this.name = name;
            this.partitions = partitions;
        }
        
        public String getName() {
            return name;
        }
        
        public List<PartitionInfo> getPartitions() {
            return new ArrayList<>(partitions);
        }
        
        @Override
        public String toString() {
            return "TopicMetadata{name='" + name + "', partitions=" + partitions + "}";
        }
    }

    public static class PartitionInfo {
        private final int id;
        private final int leader;
        private final List<Integer> followers;
        
        public PartitionInfo(int id, int leader, List<Integer> followers) {
            this.id = id;
            this.leader = leader;
            this.followers = followers;
        }
        
        public int getId() {
            return id;
        }
        
        public int getLeader() {
            return leader;
        }
        
        public List<Integer> getFollowers() {
            return new ArrayList<>(followers);
        }
        
        @Override
        public String toString() {
            return "PartitionInfo{id=" + id + ", leader=" + leader + ", followers=" + followers + "}";
        }
    }
}