package com.michalmm.kafka.ws.products.service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.michalmm.kafka.ws.products.rest.CreateProductRestModel;

@Service
public class ProductServiceImpl implements ProductService{

	@Autowired
	private KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
	
	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass()); 
	
	
	public ProductServiceImpl() {}
	
	public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}
	
	@Override
	public String createProduct(CreateProductRestModel productRestModel) {
		String productId = UUID.randomUUID().toString();
		
		// TODO: persist products details into database
		// table before publishing an event
		
		ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(
				productId, 
				productRestModel.getTitle(),
				productRestModel.getPrice(),
				productRestModel.getQuantity());
		
		// sends asynchronously 
		CompletableFuture<SendResult<String, ProductCreatedEvent>> future = kafkaTemplate
				.send("product-created-events-topic", productId, productCreatedEvent);
		
		future.whenComplete((result, exception) -> {
			if(exception != null) {
				LOGGER.error("******* Failed to send message: " + exception.getMessage());
			} else {
				LOGGER.info("******* Message sent succesfully: " + result.getRecordMetadata());
			}
		});
		
		// this makes it synchronous call
//		future.join();
		
		LOGGER.info("***** Returning product id");
		
		return productId;
	}

	
}
