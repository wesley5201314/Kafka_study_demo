package com.kafka.demo.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListener;

public class KafkaConsumerService implements MessageListener<Integer, String>{
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
	
	public void onMessage(ConsumerRecord<Integer, String> arg0) {
		logger.info("---------------Consumer---Message-------------"+arg0);
		System.err.println("-----Consumer---Message--:"+arg0.value());
	}

}
