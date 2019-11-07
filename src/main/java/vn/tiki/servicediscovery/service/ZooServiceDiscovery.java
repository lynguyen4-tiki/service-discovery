package vn.tiki.servicediscovery.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import vn.tiki.servicediscovery.listener.ServiceDiscoveryListener;
import vn.tiki.servicediscovery.model.ServiceInfo;

/**
 *
 */
@Slf4j
public class ZooServiceDiscovery implements ServiceDiscovery {
    public static final int CONNECTION_TIMEOUT_MILI = 5000;

    private ZooKeeper zookeeper;
    private CountDownLatch latch;
    private String connectionString;
    private String registryPath;
    private ServiceInfo info;
    private int timeOutConnection;
    private List<ServiceDiscoveryListener> listeners;

    @Getter
    private Map<String, ServiceInfo> nodeInfos;

    public ZooServiceDiscovery(String connectionString, String registryPath, ServiceInfo info) {
        this(connectionString, CONNECTION_TIMEOUT_MILI, registryPath, info);
    }

    public ZooServiceDiscovery(String connectionString, int timeOutConnection, String registryPath, ServiceInfo info) {
        this.connectionString = connectionString;
        this.registryPath = registryPath;
        this.timeOutConnection = timeOutConnection;
        nodeInfos = new HashMap<String, ServiceInfo>();
        info.setId(info.getId().replace("/", ""));
        this.info = info;
        listeners = new ArrayList<>();
    }

    @Override
    public void addListener(ServiceDiscoveryListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(ServiceDiscoveryListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void start() throws KeeperException, InterruptedException, NodeExistsException {
        zookeeper = connectServer(connectionString);

        if (info != null) {
            zookeeper.create(String.format("%s/%s", registryPath, info.getId()), info.getInfo().getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
    }

    @Override
    public void changeData(ServiceInfo data) {
        if (info != null && data.getId().equals(info.getId())) {
            try {
                info.setInfo(data.getInfo());
                zookeeper.setData(String.format("%s/%s", registryPath, info.getId()), info.getInfo().getBytes(), -1);
            } catch (KeeperException | InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public void unregister() {
        try {
            if (info != null) {
                zookeeper.delete(String.format("%s/%s", registryPath, info.getId()), -1);
            }
        } catch (InterruptedException | KeeperException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void reconnect() {
        zookeeper = connectServer(connectionString);
        if (zookeeper != null) {
            monitorNode();
        }
    }

    private ZooKeeper connectServer(String connectionString) {
        ZooKeeper zk = null;
        try {
            latch = new CountDownLatch(1);
            zk = new ZooKeeper(connectionString, timeOutConnection, new Watcher() {

                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.None) {
                        if (event.getState() == Event.KeeperState.SyncConnected) {
                            latch.countDown();
                            log.info("Connected to zookeeper");
                            monitorNode();
                            return;
                        }

                        if (event.getState() == Event.KeeperState.Expired
                                || event.getState() == Event.KeeperState.Disconnected) {
                            log.info("Reconnecting to zookeeper...");
                            reconnect();
                            return;
                        }
                    }
                }
            });
            latch.await();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return zk;
    }

    private void monitorNode() {
        try {
            var nodeList = zookeeper.getChildren(this.registryPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeChildrenChanged) {
                        String pathChanged = event.getPath();
                        if (registryPath.equals(pathChanged))
                            monitorNode();
                        return;
                    }
                }
            });

            for (String node : nodeList) {
                Watcher watcher = null;

                if (!this.nodeInfos.containsKey(node)) {
                    watcher = new Watcher() {

                        @Override
                        public void process(WatchedEvent event) {
                            if (event.getType() == Event.EventType.NodeDeleted) {
                                String path = event.getPath();
                                String node = getNameFromPath(path);
                                removeNode(node);
                                return;
                            }

                            if (event.getType() == Event.EventType.NodeDataChanged) {
                                String path = event.getPath();
                                String node = getNameFromPath(path);
                                try {
                                    var bytes = zookeeper.getData(path, this, null);
                                    var data = "";
                                    if (bytes != null)
                                        data = new String(bytes);
                                    var serviceInfo = new ServiceInfo(node, data);
                                    nodeInfos.put(node, serviceInfo);
                                    listeners.forEach(listener -> {
                                        listener.notifyInfoChanged(serviceInfo);
                                    });
                                    log.info("A node has data changed: {}", serviceInfo);
                                } catch (KeeperException | InterruptedException e) {
                                    log.debug(e.getMessage(), e);
                                }
                                return;
                            }
                        }
                    };
                }
                var bytes = zookeeper.getData(registryPath + "/" + node, watcher, null);
                var data = "";
                if (bytes != null)
                    data = new String(bytes);
                var serviceInfo = new ServiceInfo(node, data);
                this.nodeInfos.put(node, serviceInfo);
                if (watcher != null) {
                    listeners.forEach(listener -> {
                        listener.notifyAddedNode(serviceInfo);
                    });
                    log.info("A new node: {}", serviceInfo);
                }
            }

            infoRegistry();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void removeNode(String id) {
        var serviceInfo = nodeInfos.get(id);
        if (serviceInfo != null) {
            nodeInfos.remove(id);
            listeners.forEach(listener -> {
                listener.notifyDeletedNode(serviceInfo);
            });
            log.info("A node is removed: {}", serviceInfo);
        }
    }

    private String getNameFromPath(String path) {
        if (path == null)
            return null;
        int pos = path.lastIndexOf("/");
        if (pos < 0)
            return path;
        return path.substring(pos + 1);
    }

    private void infoRegistry() {
        StringBuilder str = new StringBuilder("All datanodes activing: ");
        for (Map.Entry<String, ServiceInfo> entry : nodeInfos.entrySet()) {
            if (entry.getKey().equals(info.getId()))
                str.append(String.format("\n%s(me) ", entry.getValue()));
            else
                str.append(String.format("\n%s ", entry.getValue()));
        }
        log.debug(str.toString());
    }
}
