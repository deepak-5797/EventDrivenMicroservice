package com.microservicess.demo.kafka.admin.config.client;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import com.microservicess.demo.kafka.admin.exception.KafkaRuntimeException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class KafkaAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;

    private final RetryConfigData retryConfigData;

    private final AdminClient adminClient;

    private final RetryTemplate retryTemplate;

    public KafkaAdminClient(KafkaConfigData kafkaConfigData,
                            RetryConfigData retryConfigData,AdminClient adminClient,RetryTemplate retryTemplate) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData  = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
    }

    public void createTopics()
    {
        CreateTopicsResult createTopicsResult;

        try{
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        }
        catch(Throwable e)
        {
            throw new KafkaRuntimeException("Reached max number of retry for kafka topics");
        }
        checkTopicsCreated();
    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext)
    {
        List<String> kafkaTopics = kafkaConfigData.getTopicNamesToCreate();
        logger.info("Creating topic result" + kafkaTopics.size() + " with retry = " + retryContext.getRetryCount());
        List<NewTopic> kafkaTopic =  kafkaTopics.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).collect(Collectors.toList());

        return adminClient.createTopics(kafkaTopic);
    }

    private Collection<TopicListing> getTopic()
    {
        Collection<TopicListing> topics;

        try {
            topics = retryTemplate.execute(this::doGetTopics);
        }
        catch (Exception e)
        {
            throw new KafkaRuntimeException("Reached max number of retry for kafka topics");
        }
        return topics;
    }

    private <T> doGetTopics()
    {

    }

    public void checkTopicsCreated()
    {

    }

}
