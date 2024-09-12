package org.e4s.client.config;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Metrics {

    @Bean
    public MeterRegistry meterRegistry() {
        return new JmxMeterRegistry(s -> null, Clock.SYSTEM);
    }
}
