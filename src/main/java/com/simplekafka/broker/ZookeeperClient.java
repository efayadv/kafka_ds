package com.simplekafka.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.w3c.dom.events.Event;

import java.util.logging.Level;

import java.util.logging.Logger; 


public class ZookeeperClient implements Watcher{

    private static final int SESSION_TIMEOUT = 3000;
    private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());

    private final String host;
    private final int port;

    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public ZookeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException, InterruptedException { //as is
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await();
        
        // Create required paths if they don't exist
        createPath("/brokers");
        createPath("/topics");
        createPath("/controller");
    }

    public String getConnectString() {
        return host + ":" + port;
    }

    public boolean exists(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        return stat != null;
    }

    public void close() throws KeeperException, InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    public void createPath(String path) {
        //zooKeeper.create()
        try {
            //base case
            if (path.equals("/")) {
                return;
            }
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash > 0) {
                String parentPath = path.substring(0, lastSlash);
                createPath(parentPath);
            }
            
            if (zooKeeper.exists(path, false) == null){
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.info("Created path: " + path);
            }

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to create path " + path);
        }
    }

    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } else {
            zooKeeper.setData(path, data.getBytes(), -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node: " + path);
            return true;
        } else {
            LOGGER.info("Ephemeral node already exists: " + path);
            return false;
        }
    }

    public String getData(String path) throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(path, false, null);
        return new String(data);
    }

    public void setData(String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data.getBytes(), -1);
    }

    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        try {
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            return new ArrayList<>();
        }
    }

    //should delete a persistent node explicitly or delete an ephemeral node when the client session ends
    public void deleteNode(String path) throws KeeperException, InterruptedException {
        if (exists(path)) {
            zooKeeper.delete(path, -1);
            LOGGER.info("Nod");
        }
    }

    /**
     * used for:
     * - detecting new brokers joining the cluster (watch /brokers)
     * - monitoring changes in topic configuration (watch /topics)
     * - tracking consumer group membership (watch /consumers/[group]/ids)
     * 
     * @param path
     * @param callback
     */
    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        List<String> newChildren = zooKeeper.getChildren(path, event2 -> {
                            if (event2.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                                watchChildren(path, callback);
                            }
                        });
                        callback.onChildrenChanged(newChildren);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error processing children changed event", e);
                    }
                }
            });
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
        }
    }

    public void watchNode(String path, NodeCallback callback) {
        try {
            zooKeeper.exists(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted ||
                    event.getType() == Watcher.Event.EventType.NodeDataChanged ||
                    event.getType() == Watcher.Event.EventType.NodeCreated) {
                    callback.onNodeChanged();
                }
            });
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            LOGGER.info("Connected to ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            LOGGER.warning("Disconnected from ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            LOGGER.warning("ZooKeeper session expired, reconnecting...");
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }

    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    public interface NodeCallback {
        void onNodeChanged();
    }
}
