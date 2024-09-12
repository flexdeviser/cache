package org.e4s.datacenter.cache.rest;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.PathParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Store {

    @Autowired
    private HazelcastInstance hzClient;

    @Autowired
    private List<UUID> keys;

    @GetMapping("/pq/{device}")
    byte[] fetchDeviceData(@PathVariable("device") String device) {
        IMap<UUID, byte[]> pqCache = hzClient.getMap("pq");
        return pqCache.get(UUID.fromString(device));
    }

}
