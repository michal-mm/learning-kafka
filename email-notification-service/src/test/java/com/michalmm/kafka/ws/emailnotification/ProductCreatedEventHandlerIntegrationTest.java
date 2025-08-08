package com.michalmm.kafka.ws.emailnotification;

import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties="spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductCreatedEventHandlerIntegrationTest {

	
}
