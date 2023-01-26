package com.example.demo;


import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;



@Service (value = "errorHandler")
public class ErrorHandler implements ConsumerAwareListenerErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorHandler.class);


    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
        LOG.warn("Exception while handling kafka record : {}, because : {}", message.getPayload(),
                exception.getMessage());

        exception.printStackTrace();

        return null;
    }
}