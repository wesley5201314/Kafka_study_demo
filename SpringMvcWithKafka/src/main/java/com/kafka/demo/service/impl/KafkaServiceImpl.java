package com.kafka.demo.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Service;

import com.kafka.demo.service.KafkaService;

@Service
public class KafkaServiceImpl implements KafkaService{
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaServiceImpl.class);
	@Autowired
	@Qualifier("kafkaTopicTest")
	private MessageChannel messageChannel;

	@Override
	public void sendInfo(String topic, Object obj) {
		logger.info("---Service:KafkaService------sendInfo------");
		System.err.println("----------Service:KafkaService------sendInfo----------"+topic);
		messageChannel.send(MessageBuilder.withPayload(obj).setHeader(KafkaHeaders.TOPIC,topic).build());
	}

}
