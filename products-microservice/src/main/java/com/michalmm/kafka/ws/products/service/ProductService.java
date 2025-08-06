package com.michalmm.kafka.ws.products.service;

import com.michalmm.kafka.ws.products.rest.CreateProductRestModel;

public interface ProductService {
	
	public String createProduct(CreateProductRestModel productRestModel);
}
