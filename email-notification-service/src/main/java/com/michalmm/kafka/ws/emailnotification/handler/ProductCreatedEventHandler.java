package com.michalmm.kafka.ws.emailnotification.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.michalmm.kafka.ws.core.ProductCreatedEvent;
import com.michalmm.kafka.ws.emailnotification.error.NotRetryableException;

@Component
@KafkaListener(topics="product-created-events-topic")
public class ProductCreatedEventHandler {
	
	private Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@KafkaHandler
	public void handle(ProductCreatedEvent productCreatedEvent) {
		LOGGER.info("Received a new event: " + productCreatedEvent.getTitle());
//		way to see if messages that cause non retryable exception end up going to DLT -> yes, they do!
//		if(true) throw new NotRetryableException("An error took place. Skipping this message");
	}
}
