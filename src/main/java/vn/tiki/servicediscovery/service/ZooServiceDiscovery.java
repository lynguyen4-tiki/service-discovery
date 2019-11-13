package vn.tiki.servicediscovery.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.WatcherType;
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
    public static final int SESSION_TIMEOUT_MILI = 2000;
    public static final int CONNECTION_TIMEOUT_MILI = 5000;

    private ZooKeeper zookeeper;
    private CountDownLatch latch;
    private String connectionString;
    private String registryPath;
    private ServiceInfo info;
    private int sessionTimeOut;
    private int connectionTimeOut;
    private List<ServiceDiscoveryListener> listeners;

    private boolean firstInit = true;

    @Getter
    private Map<String, ServiceInfo> nodeInfos;

    public ZooServiceDiscovery(String connectionString, String registryPath, ServiceInfo info) {
        this(connectionString, SESSION_TIMEOUT_MILI, CONNECTION_TIMEOUT_MILI, registryPath, info);
    }

    public ZooServiceDiscovery(String connectionString, int sessionTimeOut, int connectionTimeOut, String registryPath,
            ServiceInfo info) {
        this.connectionString = connectionString;
        this.registryPath = registryPath;
        this.sessionTimeOut = sessionTimeOut;
        this.connectionTimeOut = connectionTimeOut;
        nodeInfos = new HashMap<String, ServiceInfo>();
        if (info != null && info.getId() != null)
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
    public void stop() throws Exception {
        stop(null);
    }

    @Override
    public void stop(Runnable callbackWhenFinish) throws Exception {
        if (zookeeper != null) {
            zookeeper.removeAllWatches(registryPath, WatcherType.Children, true);
            zookeeper.close();
        }
        if (callbackWhenFinish != null)
            callbackWhenFinish.run();
    }

    @Override
    public void start() throws Exception {
        start(null);
    }

    @Override
    public void start(Runnable callbackWhenFinish) throws Exception {
        connectServer(connectionString);

        if (info != null) {
            if (info.getId() != null) {
                String path = String.format("%s/%s", registryPath, info.getId());
                createAncestorNode(path.substring(0, path.lastIndexOf("/")));
                zookeeper.create(path, info.getInfo().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
                String path = String.format("%s/", registryPath);
                createAncestorNode(path.substring(0, path.lastIndexOf("/")));
                var stat = zookeeper.create(path, info.getInfo().getBytes(), Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
                info.setId(stat.substring(stat.lastIndexOf("/") + 1));
            }
        }

        if (callbackWhenFinish != null)
            callbackWhenFinish.run();
    }

    @Override
    public void changeData(ServiceInfo data) {
        if (info != null && data.getId().equals(info.getId())) {
            try {
                info.setInfo(data.getInfo());
                String path = String.format("%s/%s", registryPath, info.getId());
                zookeeper.setData(path, info.getInfo().getBytes(), -1);
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

    private void createAncestorNode(String path) throws KeeperException, InterruptedException {
        if (path.isEmpty())
            return;
        if (zookeeper.exists(path, false) != null)
            return;
        createAncestorNode(path.substring(0, path.lastIndexOf("/")));
        try {
            zookeeper.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            log.debug("Node {} existed, have not to create", path);
        }

    }

    private void reconnect() throws IOException, InterruptedException, TimeoutException {
        connectServer(connectionString);
    }

    private void connectServer(String connectionString) throws IOException, InterruptedException, TimeoutException {

        latch = new CountDownLatch(1);
        zookeeper = new ZooKeeper(connectionString, sessionTimeOut, new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.None) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {

                        log.info("Connected to zookeeper");
                        try {
                            createAncestorNode(registryPath);
                        } catch (KeeperException | InterruptedException e) {
                            log.error(e.getMessage(), e);
                        }
                        nodeInfos.clear();
                        monitorNode();
                        latch.countDown();
                        return;
                    }

                    if (event.getState() == Event.KeeperState.Expired
                            || event.getState() == Event.KeeperState.Disconnected) {
                        log.info("Reconnecting to zookeeper...");
                        try {
                            reconnect();
                        } catch (IOException | InterruptedException | TimeoutException e) {
                            log.error(e.getMessage(), e);
                        }
                        return;
                    }
                }
            }
        });

        if (firstInit && !latch.await(connectionTimeOut, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException(
                    String.format("Time out to connect to zookeeper after %d ms", connectionTimeOut));
        }else {
            latch.await();
        }
        
        firstInit = false;

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
            if (info != null && entry.getKey().equals(info.getId()))
                str.append(String.format("\n%s(me) ", entry.getValue()));
            else
                str.append(String.format("\n%s ", entry.getValue()));
        }
        log.debug(str.toString());
    }
}
