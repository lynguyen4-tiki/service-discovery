package vn.tiki.servicediscovery;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;
import vn.tiki.servicediscovery.model.ServiceInfo;
import vn.tiki.servicediscovery.service.ZooServiceDiscovery;

@Slf4j
public class ZooSDTest {
    public static final String CONNECTION_STRING = "localhost:2181";
    public static final String MONITOR_NODE = "/test/zoo";
    public static final int SLEEP_TIME = 500;

    @Test
    public void testCreateFullInfoNode() throws IOException, KeeperException, InterruptedException {
        try {
            ServiceInfo info = new ServiceInfo("0", "localhost");
            var instance = new ZooServiceDiscovery(CONNECTION_STRING, MONITOR_NODE, info);
            assertTrue(instance.getNodeInfos().get("0") == null);
            instance.start(null);
            Thread.sleep(SLEEP_TIME);
            assertTrue(instance.getNodeInfos().get("0") != null);
            instance.stop(null);
            Thread.sleep(SLEEP_TIME);
            assertTrue(instance.getNodeInfos().get("0") == null);
        } catch (NodeExistsException e) {
            log.error("Can not create node because the id has already existed");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Test
    public void testNotCreateNode() throws IOException, KeeperException, InterruptedException {
        try {
            var instance = new ZooServiceDiscovery(CONNECTION_STRING, MONITOR_NODE, null);
            instance.start(null);

            assertTrue(instance.getNodeInfos().isEmpty());

            Thread.sleep(SLEEP_TIME);
            instance.stop(null);
            Thread.sleep(SLEEP_TIME);

            assertTrue(instance.getNodeInfos().isEmpty());
        } catch (NodeExistsException e) {
            log.error("Can not create node because the id has already existed");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Test
    public void testCreateNodeRandomId() throws IOException, KeeperException, InterruptedException {
        try {
            ServiceInfo info = new ServiceInfo(null, "localhost");
            var instance = new ZooServiceDiscovery(CONNECTION_STRING, MONITOR_NODE, info);
            instance.start(null);

            Thread.sleep(SLEEP_TIME);

            instance.stop(null);
            Thread.sleep(SLEEP_TIME);

            assertTrue(info.getId() != null);
        } catch (NodeExistsException e) {
            log.error("Can not create node because the id has already existed");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
