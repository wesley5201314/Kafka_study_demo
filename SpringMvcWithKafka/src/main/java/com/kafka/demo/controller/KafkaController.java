package com.kafka.demo.controller;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.kafka.demo.service.KafkaService;

@Controller
@RequestMapping("/kafka")
public class KafkaController {

	private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
	@Resource
	private KafkaService kafkaService;
	
	@RequestMapping("/test")
	public String test(){
		logger.info("-------KafkaController--------start-----");
		System.err.println("---------KafkaController--------start---------");
		kafkaService.sendInfo("testTopic","kafka sendMessage test!");
		return "index";
	}
}
