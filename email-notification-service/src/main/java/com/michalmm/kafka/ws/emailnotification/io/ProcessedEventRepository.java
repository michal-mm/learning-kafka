package com.michalmm.kafka.ws.emailnotification.io;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long>{

	public ProcessedEventEntity findByMessageId(String messageId);
}
