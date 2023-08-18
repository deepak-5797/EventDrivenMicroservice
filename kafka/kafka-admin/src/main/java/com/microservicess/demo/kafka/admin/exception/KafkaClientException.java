package com.microservicess.demo.kafka.admin.exception;

public class KafkaClientException extends RuntimeException{

    public KafkaClientException()
    {

    }

    public KafkaClientException(String message)
    {
        super(message);
    }

    public KafkaClientException(String message, Throwable clause)
    {
        super(message,clause);
    }
}
