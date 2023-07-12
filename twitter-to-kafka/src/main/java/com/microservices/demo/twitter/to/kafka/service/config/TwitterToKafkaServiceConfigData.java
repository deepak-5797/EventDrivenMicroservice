package com.microservices.demo.twitter.to.kafka.service.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Slf4j
@Data
@Configuration
@ConfigurationProperties(prefix="twitter-to-kafka-service")
public class TwitterToKafkaServiceConfigData {
    private List<String> twitterKeywords;
    private String welcomeMessage;
    private  Boolean enableMockTweets;
    private  Long mockSleepMs;
    private Integer mockMinTweetLength;
    private Integer mockMaxTweetLength;


    private static final Logger logger = LoggerFactory.getLogger(TwitterToKafkaServiceConfigData.class);
}
