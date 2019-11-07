package vn.tiki.servicediscovery.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ServiceInfo {
    String id;
    String info;

    @Override
    public String toString() {
        return String.format("[%s-%s]", id, info);
    }
}
