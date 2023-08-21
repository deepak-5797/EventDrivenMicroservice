package com.microservicess.demo.kafka.admin.config.client;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import com.microservicess.demo.kafka.admin.config.WebClientConfig;
import com.microservicess.demo.kafka.admin.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.http.HttpClient;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class KafkaAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;

    private final RetryConfigData retryConfigData;

    private final AdminClient adminClient;

    private final RetryTemplate retryTemplate;

    private final WebClient webClient;


    public KafkaAdminClient(KafkaConfigData kafkaConfigData,
                            RetryConfigData retryConfigData,AdminClient adminClient,RetryTemplate retryTemplate
            ,WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData  = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    public void createTopics()
    {
        CreateTopicsResult createTopicsResult;

        try{
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        }
        catch(Throwable e)
        {
            throw new KafkaClientException("Reached max number of retry for kafka topics");
        }
        checkTopicsCreated();
    }

    public void checkTopicsCreated(){
        Collection<TopicListing> topics =  getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        Integer multipler = retryConfigData.getMultipler().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        for(String topic: kafkaConfigData.getTopicNamesToCreate()) {
            while(!isTopicCreated(topics,topic)){
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs += multipler;
            }
        }
    }

    public void schemaRegistry(){
        int retryCount =1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultipler().intValue();
        Long sleepTimeMs =  retryConfigData.getSleepTimeMs();

        while(getSchemaRegistryStatus().is2xxSuccessful()){
                checkMaxRetry(retryCount++,maxRetry);
                sleep(sleepTimeMs);
            sleepTimeMs *=multiplier;
        }
    }


    private HttpStatus getSchemaRegistryStatus()
    {
        try {
            return (HttpStatus) webClient.method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange().map(ClientResponse::statusCode).block();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }

    private void sleep(Long sleepTimeMs) {
        try{
            Thread.sleep(sleepTimeMs);
        }
        catch (Exception e)
        {
            throw new KafkaClientException("Error while sleeping for waiting new created topics!!");
        }
    }

    private void checkMaxRetry(int i, Integer maxRetry) {
        if(i>maxRetry)
        {
            throw new KafkaClientException("Reached max number of rety for reading kafka topic(s)!");
        }
    }

    private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
        if(topics==null)
        {
            return false;
        }
        return topics.stream().anyMatch(topic -> topic.name().equals(topicName));
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

    private Collection<TopicListing> getTopics()
    {
        Collection<TopicListing> topics;

        try {
            topics = retryTemplate.execute(this::doGetTopics);
        }
        catch (Exception e)
        {
            throw new KafkaClientException("Reached max number of retry for kafka topics");
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException,InterruptedException
    {
        logger.info("Reading kafka topic {}, attemp {}",kafkaConfigData.getTopicNamesToCreate().toArray(),retryContext.getRetryCount());
        Collection<TopicListing>  topics = adminClient.listTopics().listings().get();

        if(topics!=null)
        {
            topics.forEach(topic -> logger.info("Topic with name {}",topic.name()));
        }
        return topics;
    }



}
