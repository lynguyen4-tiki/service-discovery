package vn.tiki.servicediscovery.service;

import vn.tiki.servicediscovery.listener.ServiceDiscoveryListener;
import vn.tiki.servicediscovery.model.ServiceInfo;

public interface ServiceDiscovery {

    public void start() throws Exception;

    public void start(Runnable callbackWhenFinish) throws Exception;

    public void changeData(ServiceInfo data);

    public void addListener(ServiceDiscoveryListener listener);

    public void removeListener(ServiceDiscoveryListener listener);

    public void unregister();

    public void stop() throws Exception;

    public void stop(Runnable callbackWhenFinish) throws Exception;
}
