package main.java.com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import main.java.com.simplekafka.broker.Protocol.ProduceResult;

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
        for (int i = 1; i <= messageCount; i++) {


        }


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

}
