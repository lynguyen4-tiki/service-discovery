package vn.tiki.servicediscovery.listener;

import vn.tiki.servicediscovery.model.ServiceInfo;

public interface ServiceDiscoveryListener {
    public void notifyDeletedNode(ServiceInfo info);
    
    public void notifyAddedNode(ServiceInfo info);
    
    public void notifyInfoChanged(ServiceInfo info);
}
