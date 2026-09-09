package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;

public class SimpleKafkaBroker {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());
    private static final String DATA_DIR = "data";

    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isController;
    private final ExecutorService executor;
    private final ZookeeperClient zkClient;
    private final ConcurrentHashMap topics;
    private final ServerSocketChannel serverChannel;
    private final Map<Integer, BrokerInfo> clusterMetadata;
    //gotta add the rest

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(10);
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();
        
        // Initialize data directory
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        // Initialize ZooKeeper client
        this.zkClient = new ZookeeperClient("localhost", zkPort);
    }

    public void start() throws IOException { 
        /*
        Sets up the network socket
        Registers the broker with ZooKeeper
        Participates in controller election
        Loads existing topic metadata
        Starts accepting client connections
         */

        //set up serverChannel
        if (isRunning.compareAndSet(false, true)) {
            serverChannel.socket().bind(InetSocketAddress(brokerHost, brokerPort));
            serverChannel.configureBlocking(false);

            LOGGER.info("SimpleKafka broker started on " + brokerHost + ":" + brokerPort);

            //register with zookeper
            registerWithZookeeper();

            //participates in controller election, another method
            electController();

            //loading existing topic metadata
            loadTopics();

            //starts accepting client connections
            executor.submit(this::acceptConnections);

        }
    }

    public void stop() {
        /*
        shuts down broker by:
        Closing the network server socket
        Closing all partition log files
        Shutting down the thread pool
        Closing the ZooKeeper connection
         */

        
        if (isRunning.compareAndSet(true, false)) {
            try {
                LOGGER.info("Stopping SimpleKafka broker...");

                //closing network server socket
                serverChannel.close();

                //closing all partition log files
                for (List<Partition> partitions : topics.values()) {
                    for (Partition partition : partitions) {
                        partition.close();
                    }
                }

                //shutting down thread pool
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);

                //Close Zk connection
                zkClient.close();

                LOGGER.info("SimpleKafka Broker stopped"); //change name?
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to stop broker", e);
            }   
        }
    }

    private SocketAddress InetSocketAddress(String brokerhost2, int brokerport2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'InetSocketAddress'");
    }

    private void registerWithZookeeper() throws IOException, InterruptedException {
        /*
        Connects to ZooKeeper
        Creates an ephemeral node for this broker (ephemeral nodes disappear when the connection is lost)
        Adds the broker’s information to local metadata
        Sets up a watch on the broker registry to detect cluster changes
         */
        try {
            zkClient.connect();
            //creates an ephemeral node for broker
            String brokerPath = "/brokers/" + brokerId;
            String brokerData = brokerHost + ":" + brokerPort; 
            zkClient.createEphemeralNode(brokerPath, brokerData);

            //adding broker information to local metadata
            BrokerInfo selfInfo = new BrokerInfo(brokerId, brokerHost, brokerPort);
            clusterMetadata.put(brokerId, selfInfo);

            //setting up watch on the broker registry
            zkClient.watchChildren("/brokers", this::onBrokersChanged);

            LOGGER.info("Registered with Zookeeper at " + zkClient.getConnectString());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with Zookeeper", e);
        }
        
    }

    private void electController() {
        /*
        This method implements a controller election process:
        Checks if a controller already exists in ZooKeeper
        Attempts to create a controller node if none exists
        If successful, becomes the controller and rebalances partitions
        If not, watches for controller changes
        Retries after failures with a delay
         */
        try {
            String controllerPath = "/controller";

            //check if controller exists in zookeeper
            boolean nodeExists = zkClient.exists(controllerPath);
            if (nodeExists) { //if true, it becomes the controller and rebalances partitions
                String existingData = zkClient.getData(controllerPath);
                if (existingData == null || existingData.trim().isEmpty()) {
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                }
            }

            //create controller
            boolean becameController = false;
            if (!nodeExists) {
                becameController = zkClient.createEphemeralNode(controllerPath, String.valueOf(brokerId));
            }
            if (becameController) {
                isController.set(true);
                rebalancePartitions();
            } else {
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }
        
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed", e);
            new Thread(() -> { //stronger implementation would use ScheduleExecutorService
                try { 
                    Thread.sleep(2000);
                    electController();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }).start(); 
        }
    }

    private void loadTopic(String topic) throws Exception {
        /*
        Checks if topic already exists locally
        Verifies topic exists in ZooKeeper
        Creates local directory structure for the topic
        Reads partition metadata from ZooKeeper
        Creates partition objects with leader and follower information
        Adds topic to local metadata
         */
        //checking existance locally
        if (topics.containsKey(topic)) {
            LOGGER.info("Topic already loaded: " + topic);
            return;
        }
        String path = "/topics/" + topic;
        if (!zkClient.exists(path)) {
            throw new Exception("Topic does not exist in Zookeeper: " + topic);
        }

        //creating local directory structure for topic
        String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
        new File(topicDir).mkdirs();

        //reading partition metadata from zk
        List<String> partitionIds = zkClient.getChildren(path + "/partitions");
        List<Partition> partitions = new ArrayList<>();

        for (String partitionId : partitionIds) {
            //gotta parse them into partitions
            int id = Integer.parseInt(partitionId);
            String partitionPath = path + "/partitions/" + partitionId;
            String partitionData = zkClient.getData(partitionPath);
            //extract leader and followers
            String[] elements = partitionData.split(";");
            int leader = Integer.parseInt(elements[0]);

            List<Integer> followers = new ArrayList<>();
            if (elements.length > 1 && !elements[1].isEmpty()) {
                String[] followerIds = elements[1].split(";");
                for (String follower : followerIds) {
                    //parse the follower to int before adding to followers list
                    if (!follower.isEmpty()) {
                        int f = Integer.parseInt(follower);
                        followers.add(f);
                    }
                }
            }

            String partitionDir = topicDir + File.separator + id;
            new File(partitionDir).mkdirs();

            Partition partition = new Partition(id, leader, followers, partitionDir);
            partitions.add(partition);

            LOGGER.info("Loaded partition " + id + " for topic " + topic +
                    ", leader: " + leader + ", followers: " + followers);
        }

        //adding topic to local
        topics.put(topic, partitions);
        LOGGER.info("Successfully loaded topic: " + topic + " with " + partitions.size() + " partitions");
    }

    public void loadTopics() {
        try {
            List<String> topicNames = zkClient.getChildren("/topics");

            for (String topic : topicNames) {
                try {
                    loadTopic(topic);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failed to load topic" + topic, e);
                }
            }
            LOGGER.info("Loaded " + topics.size() + " topics");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topics", e);
        }
    }

    private void rebalancePartitions() {
        /*
        Ensures only the controller performs rebalancing
        Checks each partition for valid leaders
        Assigns new leaders for partitions with missing leaders
        Updates follower assignments
        Updates partition metadata in ZooKeeper
         */
        //only the controller can perform rebalacing
        if (!isController.get()) {
            return;
        }

        LOGGER.info("Rebalancing partitions across cluster");

        //check each partition for valid leaders
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            //so each entry contains a string and a list of partitions?
            //another forloop?
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();

            for (Partition partition : partitions) {
                //checking for valid leader (valid?)
                if (partition.getLeader() == -1 || clusterMetadata.containsKey(partition.getLeader())) {
                    //assign leader
                    List<Integer> brokers = new ArrayList<>(clusterMetadata.keySet());
                    if (!brokers.isEmpty()){ //not empty
                        int newLeader = brokers.get(0);
                        partition.setLeader(newLeader);

                        //set other brokers as followers
                        List<Integer> followers = new ArrayList<>();
                        for (int i = 1; i < Math.min(brokers.size(), 3); i++) { //feel like i can come up with something better
                            followers.add(brokers.get(i));
                        }
                        partition.setFollowers(followers);

                        //update partition metadata
                        updatePartitionMetadata(topic, partition);

                        LOGGER.info("Reassigned partition " + partition.getId() +
                            " of topic " + topic +
                            " with leader " + newLeader + 
                            " with followers " + followers
                        );
                    }
                }
            }
        }
    }

    private void updatePartitionMetadata(String topic, Partition partition) {
        //update it in Zookeeper
        try {
            String path = "/topics/" + topic + "/partitions/" + partition.getId();
            String data = partition.getLeader() + ";";
            for (int follower : partition.getFollowers()) {
                data += follower + ";";
            }
            if (zkClient.exists(path)) {
                zkClient.setData(path, data);
            } else {
                //create node?
                zkClient.createPersistentNode(path, data); 
            }
        
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update partition metadata", e);
        }
    }


    private void onBrokersChanged(List<String> brokerIds) {
        // Update cluster metadata
        // Remove brokers that have disappeared
        // Re-elect controller if needed
            // As controller, rebalance partitions due to cluster changes
            // Re-attempt controller election
        
        //updating local metadata w new brokers
        //clusterMetadata takes in Integer (brokerId) and BrokerInfo (broker intself)

        //since it is a callback we start with a message
        LOGGER.info("Broker changed detected. Current brokers: " + brokerIds);

        for (String brokerId : brokerIds) {
            //check if it exists first?
            try {
                //look for the metadata in Zk
                if (clusterMetadata.containsKey(Integer.parseInt(brokerId))) {
                    String brokerPath = "/brokers/" + brokerId;
                    String brokerData = zkClient.getData(brokerPath);
                    //build a BrokerInfo
                    String[] elements = brokerData.split(":");

                    int port = Integer.parseInt(elements[1]);

                    BrokerInfo broker = new BrokerInfo(Integer.parseInt(brokerId), elements[0], port);
                
                    clusterMetadata.put(Integer.parseInt(brokerId), broker);
                    LOGGER.info("Added broker: " + broker);
                } 

            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to process broker info", e);
            }
        }

        //remove brokers that have disappeared
        List<Integer> unavailable = new ArrayList<>();
        for (Integer brokerId : clusterMetadata.keySet()) {
            if (!brokerIds.contains(String.valueOf(brokerId))) {
                unavailable.add(brokerId);
            }
        }

        for (Integer brokerId : unavailable) {
            clusterMetadata.remove(brokerId);
            LOGGER.info("Removed broker: " + brokerId);
        }

        //Reelect controller if needed
        if (!brokerIds.contains(String.valueOf(brokerId)) && isController.get()) {
            isController.set(false);
            LOGGER.info("This broker is no longer in the cluster, giving up controller status");
        } else if (isController.get()) {
            rebalancePartitions();
        } else {
            electController();
        }
    }

    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        // Create topic directory
        // Create topic in ZooKeeper
        // Create partitions
            // Select leader and followers
            // Create partition
            // Store partition metadata in ZooKeeper
        // Add topic to broker's metadata
        // Notify all brokers to load the topic

        if (!isController.get()) {
            LOGGER.info("Only the controller can create topics");
            return;
        }

        try {
            String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
            new File(topicDir).mkdirs();

            //create in Zk
            String path = "/topics/" + topic;
            if (!zkClient.exists(path)) {
                zkClient.createPersistentNode(path, ""); //persistent bc ...
                zkClient.createPersistentNode(path + "/partitions", "");
            }
            
            //create partitions
            List<Partition> partitions = new ArrayList<>();
            List<Integer> brokerIds = new ArrayList<>(clusterMetadata.keySet());
            for (int i = 0; i < numPartitions; i++) {
                int partitionId = i;
                String partitionDir = topicDir + File.separator + partitionId;
                new File(partitionDir).mkdirs();
                //select leader
                int leaderIndex = i % brokerIds.size();
                int leaderId = brokerIds.get(leaderIndex); 

                //followers
                List<Integer> followers = new ArrayList<>();
                for (int j = 0; j < replicationFactor; j++) {
                    int followerIndex = (leaderIndex + j) % brokerIds.size();
                    followers.add(brokerIds.get(followerIndex));
                }

                Partition partition = new Partition(i, leaderId, followers, partitionDir);
                partitions.add(partition);

                //store in Zk
                String partitionPath = path + "/partitions/" + partitionId;
                String partitionData = leaderId + ";";
                for (int follower : followers) {
                    partitionData += follower + ",";
                }

                zkClient.createPersistentNode(partitionPath, partitionData);

                LOGGER.info("Created partition " + partitionId +
                        " for topic " + topic +
                        " with leader " + leaderId +
                        " and followers " + followers);
            } 

            //add topic to brokers metadata
            topics.put(topic, partitions);

            //Notify brokers to load topic
            for (int brokerId : brokerIds) {
                if (brokerId != this.brokerId) {
                    notifyBrokerForTopicCreation(brokerId, topic);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create topic", e);
        }

    }

    private void notifyBrokerForTopicCreation(int brokerId, String topic) { 
        BrokerInfo broker = clusterMetadata.get(brokerId);
        if (broker == null)
            return;

        executor.submit(() -> {
            try (SocketChannel brokerChannel = SocketChannel.open()) {
                brokerChannel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));

                ByteBuffer request = ByteBuffer.allocate(3 + topic.length());
                request.put(Protocol.TOPIC_NOTIFICATION);
                request.putShort((short) topic.length());
                request.put(topic.getBytes());
                request.flip();

                brokerChannel.write(request);

                ByteBuffer response = ByteBuffer.allocate(1);
                brokerChannel.read(response);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to notify broker " + brokerId + " about topic creation", e);
            }
        });
    }

    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // Parse request data
        // Check if topic exists
        // Find the partition
        // Check if this broker is the leader for the partition
            // Forward to leader
        // Append message to log
        // Replicate to followers
        // Send acknowledgment to client

        //parsing topic, partition, message
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partition = buffer.getInt();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        LOGGER.info("Produce request for topic: " + topic + ", partition: " + partition);

        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic does not exist");
            return;
        }

        //find partition
        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;

        for (Partition p : partitions) {
            if (p.getId() == partition) {
                targetPartition = p;
                break;
            }
        }

        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        //check if broker is leader for partition
        if (targetPartition.getLeader() != brokerId) {
            //send request to broker that is leader
            forwardProduceToLeader(clientChannel, topic, partition, message, targetPartition.getLeader());
            return;
        }

        //append message to log
        long offset = targetPartition.append(message);

        //replicate to followers
        replicateToFollowers(topic, targetPartition, message, offset);

        //send ack to client
        ByteBuffer response = ByteBuffer.allocate(10);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.put((byte) (offset > -1 ? 0 : 1)); //1 is error
        response.flip();
        clientChannel.write(response);

    }


    private void forwardProduceToLeader(SocketChannel clientChannel, String topic, int partition,
            byte[] message, int leaderId) throws IOException { 
                // Locate leader broker using metadata
                // Connect to leader
                // Send equivalent produce request
                // Receive leader's response
                // Return that response to the original client
            
                BrokerInfo leader = clusterMetadata.get(leaderId);
                if (leader == null) {
                    Protocol.sendErrorResponse(clientChannel, "Leader broker not available");
                    return;
                }

                //connecting to leader
                try (SocketChannel leaderChannel = SocketChannel.open()) {
                    leaderChannel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

                    //prepare equivalent produce request
                    ByteBuffer request = ByteBuffer.allocate(9 + topic.length() + message.length);
                    request.put(Protocol.PRODUCE);
                    request.putShort((short) topic.length());
                    request.put(topic.getBytes());
                    request.putInt(partition);
                    request.putInt(message.length);
                    request.put(message);
                    request.flip();

                    //send
                    leaderChannel.write(request);

                    //Read response
                    ByteBuffer response = ByteBuffer.allocate(10);
                    leaderChannel.read(response);
                    response.flip();

                    //forward to client
                    clientChannel.write(response);
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Failed to forward produce request to leader", e);
                    Protocol.sendErrorResponse(clientChannel, "Failed to forward to leader");
                }
    }

    private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
        // Prepare replication request
        // Send request to follower
        // Read acknowledgment

        //from protocol encodeReplicateRequest

        //we need to get followers
        for (int followerId : partition.getFollowers()) {
            if (followerId == brokerId) {
                continue; //skip self
            }

            //maybe check if any followers at all?
            BrokerInfo follower = clusterMetadata.get(followerId);
            if (follower == null) {
                continue;
            }

            //connecting to follower
            executor.submit(() -> {
                try (SocketChannel followerChannel = SocketChannel.open()) {
                    followerChannel.connect(new InetSocketAddress(follower.getHost(), follower.getPort()));

                    //prepare replicate request
                    ByteBuffer request = ByteBuffer.allocate(17 + topic.length() + message.length);
                    request.put(Protocol.REPLICATE);
                    request.putShort((short) topic.length());
                    request.put(topic.getBytes());
                    request.putInt(partition.getId());
                    request.putLong(offset);
                    request.putInt(message.length);
                    request.put(message);
                    request.flip();

                    //send
                    followerChannel.write(request);

                    //get reponse
                    ByteBuffer response = ByteBuffer.allocate(1);
                    followerChannel.read(response);
                    response.flip();

                    byte acknowledgment = response.get();  
                    LOGGER.info("Replication to follower " + followerId + " " +
                            (acknowledgment == Protocol.REPLICATE_ACK ? "succeeded" : "failed"));
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Replication to follower " + followerId + " failed", e);
                }

            });

        }

    }

    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // Parse request data
        // Check if topic exists
        // Find the partition
        // Check if the offset is valid
            // No messages available at this offset
        // Read messages from log
        // Send response
                // 1 byte for response type, 4 bytes for message count
        // 8 bytes for offset, 4 bytes for length, plus message bytes
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        //partition
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();

        LOGGER.info("Fetch request for topic: " + topic + ", partition: " + partition +
                ", offset: " + offset + ", maxBytes: " + maxBytes);

        List<Partition> partitions = topics.get(topic);
        Partition targetPartition = null;

        for (Partition p : partitions) {
            if (p.getId() == partition) {
                targetPartition = p;
                break;
            }
        }

        if (targetPartition == null) {
            Protocol.sendErrorResponse(clientChannel, "Partition does not exist");
            return;
        }

        //checking if offset is valid?
        if (offset >= targetPartition.getLogEndOffset()) { //check if offset is the end of log
            //therefore, no messages available
            ByteBuffer response = ByteBuffer.allocate(5); // 1 for type 4 for message
            response.put(Protocol.FETCH_RESPONSE);
            response.putInt(0);
            response.flip();
            clientChannel.write(response);
            return;
        }

        List<byte[]> messages = targetPartition.readMessages(offset, maxBytes);
        
        int msgsSize = 0;
        for (byte[] msg : messages) {
            msgsSize += 12 + msg.length; //8 bytes offset, 4 for message length, plus message
        }

        ByteBuffer response = ByteBuffer.allocate(5 + msgsSize); //1 response type + 4 of message count
        response.put(Protocol.FETCH_RESPONSE);
        response.putInt(messages.size());

        long currentOffset = offset;
        for (byte[] msg : messages) {
            response.putLong(currentOffset);
            response.putInt(msg.length);
            response.put(msg);
            currentOffset++;
        }

        response.flip();
        clientChannel.write(response);

    }

    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        int size = 5; //1 for response type, 4 for topic count

        //calculate size for topic metadata
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            size += 6 + entry.getKey().length(); //2 bytes for length, 4 for partition count

            size += entry.getValue().size() * 12;

            for (Partition partition : entry.getValue()) {
                size += partition.getFollowers().size() * 4; //4 bytes per followerID
            }
        }

        //add size for brokers metadata
        size += 4; //broker count
        size += clusterMetadata.size() * 10; //4 bytes for brokerId, 2 bytes for broker host length, 4 bytes for port

        for (BrokerInfo broker : clusterMetadata.values()) {
            size += broker.getHost().length();
        }

        //we can now start processing response
        ByteBuffer response = ByteBuffer.allocate(size);
        response.put(Protocol.METADATA_RESPONSE);

        //broker metadata
        response.putInt(clusterMetadata.size());
        for (BrokerInfo broker : clusterMetadata.values()) {
            response.putInt(broker.getId());
            response.putShort((short) broker.getHost().length());
            response.put(broker.getHost().getBytes());
            response.putInt(broker.getPort());
        }

        // add topic metadata
        response.putInt(topics.size());
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();

            response.putShort((short) topic.length());
            response.put(topic.getBytes());
            response.putInt(partitions.size());

            for (Partition partition : partitions) {
                response.putInt(partition.getId());
                response.putInt(partition.getLeader());
                List<Integer> followers = partition.getFollowers();
                response.putInt(followers.size());
                for (Integer follower : followers) {
                    response.putInt(follower);
                }
            }
        }

        response.flip();
        clientChannel.write(response);
    }

    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        LOGGER.info("Create topic request: " + topic +
                ", partitions: " + numPartitions +
                ", replication: " + replicationFactor);

        //check if topic already exists
        if (topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic already exists");
            return;
        }

        //validate parameters
        if (numPartitions <= 0 || replicationFactor <= 0 || replicationFactor > clusterMetadata.size()) {
            Protocol.sendErrorResponse(clientChannel, "Invalid Topic Configuration");
            return;
        }

        // as controller, create topic
        if (isController.get()) {
            createTopic(topic, numPartitions, replicationFactor);

            //send success response
            ByteBuffer response = ByteBuffer.allocate(2);
            response.put(Protocol.CREATE_TOPIC_RESPONSE);
            response.put((byte) 0);
            response.flip();
            clientChannel.write(response);
        } else {
            forwardCreateTopicToController(clientChannel, topic, numPartitions, replicationFactor);
        }
    }

    private void forwardCreateTopicToController(SocketChannel clientChannel, String topic,
            int numPartitions, short replicationFactor) throws IOException {

        //find controller
        int controllerId = -1;
        try {
            String controllerData = zkClient.getData("/controller");
            controllerId = Integer.parseInt(controllerData);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get controller info", e);
            Protocol.sendErrorResponse(clientChannel, "Controller not available");
            return;
        }

        BrokerInfo controller = clusterMetadata.get(controllerId);
        if (controller == null) {
            Protocol.sendErrorResponse(clientChannel, "Controller not available");
            return;
        }

        try (SocketChannel controllerChannel = SocketChannel.open()) {
            controllerChannel.connect(new InetSocketAddress(controller.getHost(), controller.getPort()));

            ByteBuffer request = ByteBuffer.allocate(9 + topic.length());
            request.put(Protocol.CREATE_TOPIC);
            request.putShort((short) topic.length());
            request.put(topic.getBytes());
            request.putInt(numPartitions);
            request.putShort(replicationFactor);
            request.flip();

            // send request to controller
            controllerChannel.write(request);

            //read
            ByteBuffer response = ByteBuffer.allocate(2);
            controllerChannel.read(response);
            response.flip();

            clientChannel.write(response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward create topic request to controller", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to controller");
        }
    }

}
