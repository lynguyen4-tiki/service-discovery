package vn.tiki.servicediscovery;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.Test;

import lombok.extern.log4j.Log4j2;
import vn.tiki.servicediscovery.model.ServiceInfo;
import vn.tiki.servicediscovery.service.ZooServiceDiscovery;

@Log4j2
public class ZooSDTest {
    public static final String CONNECTION_STRING = "localhost:2181";
    public static final String MONITOR_NODE = "/test";

    @Test
    public void test() throws IOException, KeeperException, InterruptedException {
        try {
            ServiceInfo info = new ServiceInfo("0", "localhost");
            var instance = new ZooServiceDiscovery(CONNECTION_STRING, MONITOR_NODE, info);
            instance.start();

            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            assertTrue(true);
        } catch (NodeExistsException e) {
            log.error("Can not create node because the id has already existed");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
