package com.microservices.demo.twitter.to.kafka.service.Exception;

public class TwitterToKafkaException extends RuntimeException{

    public TwitterToKafkaException()
    {
        super();
    }
    public TwitterToKafkaException(String message)
    {
        super(message);
    }

    public TwitterToKafkaException(String message ,Throwable clause)
    {
        super(message,clause);
    }
}
