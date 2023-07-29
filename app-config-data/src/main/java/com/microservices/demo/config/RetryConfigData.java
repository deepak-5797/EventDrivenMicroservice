package com.microservices.demo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix= "retry-config")
public class RetryConfigData {

    private Long initalIntervalMs;
    private Long maxIntervalMs;
    private Double multipler;
    private Integer maxAttempts;
    private Long sleepTimeMs;

}
