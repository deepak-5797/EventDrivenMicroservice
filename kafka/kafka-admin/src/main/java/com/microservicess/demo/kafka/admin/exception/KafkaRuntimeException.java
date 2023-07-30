package com.microservicess.demo.kafka.admin.exception;

public class KafkaRuntimeException extends RuntimeException{

    public KafkaRuntimeException()
    {

    }

    public KafkaRuntimeException(String message)
    {
        super(message);
    }

    public KafkaRuntimeException(String message, Throwable clause)
    {
        super(message,clause);
    }
}
