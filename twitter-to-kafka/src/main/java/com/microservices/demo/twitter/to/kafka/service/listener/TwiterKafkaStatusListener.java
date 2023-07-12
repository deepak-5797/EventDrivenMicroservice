package com.microservices.demo.twitter.to.kafka.service.listener;

import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;


@Component
public class TwiterKafkaStatusListener extends StatusAdapter {
    private static final Logger logger = LoggerFactory.getLogger(TwiterKafkaStatusListener.class);

    @Override
    public void onStatus(Status status)
    {
        logger.info("Twitter with status :: " + status.getText());
    }

}
