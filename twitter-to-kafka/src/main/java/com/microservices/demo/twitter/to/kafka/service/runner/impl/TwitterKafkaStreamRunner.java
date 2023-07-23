package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwiterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.Arrays;

@Component
@ConditionalOnProperty(name ="twitter-to-kafka-service.enable-mock-tweets",havingValue = "false" , matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger logger = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwiterKafkaStatusListener twiterKafkaStatusListener;


    private  TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData ,TwiterKafkaStatusListener twiterKafkaStatusListener )
    {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twiterKafkaStatusListener = twiterKafkaStatusListener;
    }
    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twiterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown()
    {
        if(twitterStream!=null)
        {
            logger.info("Closing twitter stream ");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        String []keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        logger.info("Started filtering for twitter streams for keywords", Arrays.toString(keywords));
    }
}
