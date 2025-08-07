package com.michalmm.kafka.ws.emailnotification.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.michalmm.kafka.ws.core.ProductCreatedEvent;
import com.michalmm.kafka.ws.emailnotification.error.NotRetryableException;
import com.michalmm.kafka.ws.emailnotification.error.RetryableException;
import com.michalmm.kafka.ws.emailnotification.io.ProcessedEventEntity;
import com.michalmm.kafka.ws.emailnotification.io.ProcessedEventRepository;

@Component
@KafkaListener(topics="product-created-events-topic")
public class ProductCreatedEventHandler {
	
	private Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	
	private RestTemplate restTemplate;
	
	private ProcessedEventRepository processedEventRepository;
	
	
	public ProductCreatedEventHandler(RestTemplate restTemplate,
				ProcessedEventRepository processedEventrepository) {
		this.restTemplate = restTemplate;
		this.processedEventRepository = processedEventrepository;
	}
	

	@KafkaHandler
	@Transactional
	public void handle(@Payload ProductCreatedEvent productCreatedEvent,
						@Header(value="messageId", required=true) String messageId, 
						@Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {
		LOGGER.info("Received a new event: " + productCreatedEvent.getTitle() 
		+ " with productId: " + productCreatedEvent.getProductId());
		
		
		// check if this message was already processed before
		ProcessedEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);
		if(existingRecord != null) {
			LOGGER.info("MessageId: " + messageId + " already exists int he DB");
		}
//		way to see if messages that cause non retryable exception end up going to DLT -> yes, they do!
//		if(true) throw new NotRetryableException("An error took place. Skipping this message");
		
		// handle retryable exception
		String requestUrl = "http://localhost:8082/response/200";
		/*
		try {
			ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);
			if (response.getStatusCode().value() == HttpStatus.OK.value()) {
				LOGGER.info("Received response from a remote service: " + response.getBody());
			} else {
				// TODO...
			}
		} catch (ResourceAccessException ex) {
			LOGGER.error(ex.getMessage());
			throw new RetryableException(ex);
		} catch(HttpServerErrorException ex) {
			LOGGER.error("HttpServerError --> " + ex.getMessage());
			throw new NotRetryableException(ex);
		} catch (Exception ex) {
			LOGGER.error("General exception --> " + ex.getMessage());
			throw new NotRetryableException(ex);
		}
		*/
		// save unique message id in the DB
		try {
			processedEventRepository.save(new ProcessedEventEntity(messageId, productCreatedEvent.getProductId()));
		} catch(DataIntegrityViolationException ex) {
			LOGGER.error("Duplicate keys in DB, not processing again --> " + ex.getMessage());
			throw new NotRetryableException(ex);
		}
	}
}
