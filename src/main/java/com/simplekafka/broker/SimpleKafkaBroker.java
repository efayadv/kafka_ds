package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
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


        } catch () {

        }
        
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
