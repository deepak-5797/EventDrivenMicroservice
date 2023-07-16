package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.twitter.to.kafka.service.Exception.TwitterToKafkaException;
import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwiterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.*;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name ="twitter-to-kafka-service.enable-mock-tweets",havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger logger = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private static Random RANDOM = new Random();
    private TwiterKafkaStatusListener twiterKafkaStatusListener;

    public static final String WORDS[] =  new String[]{
        "Lorem",
            "Ipsum",
            "dummy",
            "printer",
            "publishing",
            "test",
            "sample",
            "string",
            "java",
            "spring"
    };

    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";


    public static  final String TWITTER_STATUS_DATE_FORMAT ="EEE MMM dd HH:mm:ss zzz yyyy";
    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData , TwiterKafkaStatusListener twiterKafkaStatusListener )
    {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twiterKafkaStatusListener = twiterKafkaStatusListener;
    }


    @Override
    public void start() throws TwitterException {
        String []keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        int maxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        long ms = twitterToKafkaServiceConfigData.getMockSleepMs();
        logger.info("Streaming Data continuously " + Arrays.toString(keywords));

        simulateTwitterStreams(keywords, minTweetLength, maxTweetLength, ms);

    }

    private void simulateTwitterStreams(String[] keywords, int minTweetLength, int maxTweetLength, long ms)  {
        Executors.newSingleThreadExecutor().submit( ()->{
           try {
               while (true) {
                   String formattedTweetAsJson = getFormateedTwetts(keywords, minTweetLength, maxTweetLength);
                   Status status = (TwitterObjectFactory.createStatus(formattedTweetAsJson));
                   twiterKafkaStatusListener.onStatus(status);
                   sleep(ms);
               }
           }
           catch (Exception e)
           {
               e.printStackTrace();
           }
        });
    }

    private void sleep(long ms) {
        try{
                Thread.sleep(ms);
        }
        catch(Exception e)
        {
            throw new TwitterToKafkaException("Error while sleeping at");
        }
    }

    private String getFormateedTwetts(String[] keywords, int minTweetLength, int maxTweetLength) {

        String parms[] = {
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords,minTweetLength,maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))

        };
        return getTweet(parms);
    }

    private static String getTweet(String[] parms) {
        String tweet = tweetAsRawJson;
        for(int i = 0; i< parms.length; i++)
        {
            tweet  = tweet.replace( "{" + i + "}", parms[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder builder = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength-minTweetLength + 1) + minTweetLength;

        return getExtractedTweet(keywords, builder, tweetLength);

    }

    private static String getExtractedTweet(String[] keywords, StringBuilder builder, int tweetLength) {
        for(int i = 0; i< tweetLength; i++)
        {
            builder.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if(i == tweetLength /2)
            {
                builder.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }

        }
        return builder.toString().trim();
    }
}
