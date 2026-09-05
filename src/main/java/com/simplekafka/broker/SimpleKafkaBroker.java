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
import java.util.concurrent.atomic.AtomicBoolean;
import java.nio.channels.ServerSocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleKafkaBroker {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());
    private static final String DATA_DIR = "data";

    private static final int brokerId;
    private static final String brokerHost;
    private static final int brokerPort;
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
                zkClient.watchNode(controllerPath, this:onControllerChange);
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
                    }
                }
            }
        }
    }

    private void updatePartitionMetadata(String topic, Partition partition) {
        

    }


    private void onBrokersChanged(List<String> brokerIds) {
        // Update cluster metadata
        // Remove brokers that have disappeared
        // Re-elect controller if needed
            // As controller, rebalance partitions due to cluster changes
            // Re-attempt controller election
    }

    public void stop() { ... }

    
}
