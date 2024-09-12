package org.e4s.datacenter.cache.config;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Utils {

    private final List<UUID> keys;

    public Utils() {
        this.keys = new ArrayList<>();
    }

    @Bean(name = "keys")
    public List<UUID> keyProvider() {
        return keys;
    }

}
