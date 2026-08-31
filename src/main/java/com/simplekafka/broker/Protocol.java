package main.java.com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import main.java.com.simplekafka.broker.Protocol.FetchResult;
import main.java.com.simplekafka.broker.Protocol.MetadataResult;
import main.java.com.simplekafka.broker.Protocol.PartitionMetadata;
import main.java.com.simplekafka.broker.Protocol.ProduceResult;
import main.java.com.simplekafka.broker.Protocol.TopicMetadata;

public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;

    //internal broker communication

    //ENCODING
    /*
    Encodes a request to produce (write) a message to a specific topic and partition
     */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        int size = 
            1 //request type
            + 2 //topic string length
            + topicBytes.length //N bytes of topic name
            + 4 //partition
            + 4 // message length
            + message.length;

        ByteBuffer buffer = ByteBuffer.allocate(size); // allocating size for our produce

        buffer.put(PRODUCE); //Type
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);

        buffer.flip();
        return buffer;

    }

    /*
    Encodes a request to fetch (read) messages from a specific topic, partition, starting from a given offset
     */
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        
        int size = 
            1 //type
            + 2 //topic string length
            + topicBytes.length //N bytes of topic names
            + 4 
            + 8
            + 4;
            //offset + maxBytes

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(FETCH);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);

        buffer.flip();
        return buffer;

    }

    /*
    Encodes a request to retrieve metadata about brokers and topics in the cluster
     */
    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        int size = 
            1
            + 2
            + topicBytes.length
            + 4
            + 2; //replication factor

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {

    }
    public static ByteBuffer encodeTopicNotification(String topic) {}

    //DECODING
    /*
    Decodes a response from a produce request
     */
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        //check if response type matches PRODUCE_RESPONSE
        byte responseType = buffer.get();

        if (responseType != PRODUCE_RESPONSE) {
            throw new IllegalArgumentException( //HANDLE ERROR RESPONSE
                "Expected PRODUCE_RESPONSE but received: " + responseType
            );
        }

        //Extracts offset and status information
        long offset = buffer.getLong();
        byte status = buffer.get();
        
        return new ProduceResult(offset, status == 0 ? null: "Produce Failed");
    }
    /**
     * Decodes response from fetch request
     * 
     * @param buffer
     * @return
     */
    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();

        if (responseType != FETCH) {
            throw new IllegalArgumentException(
                "Expected FETCH but received: " + responseType
            );
        }

        int messageCount = buffer.getInt(); //extracts message count
        byte[][] messages = new byte[messageCount][]; //byte[] is an array()

        //extracting each message with its offset and size
        //lets FIO on paper
        for (int i = 1; i < messageCount; i++) {
            long offset = buffer.getLong(); //skip
            int size = buffer.getInt();
            messages[i] = new byte[size];
            buffer.get(messages[i]);
        }

        return new FetchResult(messages, null);
    }

    /**
     * decodes response containing cluster metadata
     * 
     * @param buffer
     * @return
     */
    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        //Processes broker information (IDs, hosts, ports)
        //skip request response type?
        byte type = buffer.get(); // skip for now
        if (type != METADATA_RESPONSE) {
            throw new IllegalArgumentException( //HANDLE ERROR RESPONSE
                "Expected PRODUCE_RESPONSE but received: " + type
            );
        }
        //^GOTTA DO SOMETHING

        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();

        for (int i = 1; i < brokerCount; i++) {
            int id = buffer.getInt();
            short hostLength = buffer.getShort();
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);
            String host = new String(hostBytes);
            int port = buffer.getInt();
            brokers.add(new BrokerInfo(id, host, port));
        }

        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();
        
        for (int i = 1; i < topicCount; i++) {
            //String name = StandardCharsets.UTF_8.decode(buffer).toString();
            short nameLength = buffer.getShort();
            byte[] nameBytes = new byte[nameLength]; //destination array
            buffer.get(nameBytes);
            String name = new String(nameBytes);

            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();
            for (int j = 1; j < partitionCount; j++) {
                int partitionId = buffer.getInt();
                int leader = buffer.getInt();
                int replicasCount = buffer.getInt();
                List<Integer> replicaIds = new ArrayList<>();
                
                for (int k = 1; k < replicasCount; k++) {
                    replicaIds.add(buffer.getInt());
                }

                partitions.add(new PartitionMetadata(partitionId, leader, replicaIds));
            }
            topics.add(new TopicMetadata(name, partitions));
        }


        //should return list of broker info, list of topic metadata w/ partition info, error info
        return new MetadataResult(brokers, topics, null);
    }

    

    /**
     * 
     * ProduceResult class
     */
    public static class ProduceResult {
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public long getOffset() {
            return offset;
        }
        
        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    public static class FetchResult {
        private final byte[][] messages;
        private final String error;

        public FetchResult(byte[][] messages, String error) {
            this.messages = messages;
            this.error = error;
        }

        public byte[][] getMessages() {
            return messages;
        }

        public int getMessageCount() {
            return messages.length;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
        
    }

    public static class MetadataResult {
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }

        public List<BrokerInfo> getBrokers() {
            return brokers;
        }

        public List<TopicMetadata> getTopics() {
            return topics;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }

    }

    public static class TopicMetadata {
        private final String name;
        private final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        public List<PartitionMetadata> getPartitions() {
            return partitions;
        }

    }

    public static class PartitionMetadata {
        private final int id;
        private final int leader;
        private final List<Integer> replicas;

        public PartitionMetadata(int id, int leader, List<Integer> replicas) {
            this.id = id;
            this.leader = leader;
            this.replicas = replicas;
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getReplicas() {
            return replicas;
        }

    }
}
