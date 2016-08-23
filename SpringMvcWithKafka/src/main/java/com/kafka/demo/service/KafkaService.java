package com.kafka.demo.service;

public interface KafkaService {
	/**
     * 发消息
     * @param topic 主题
     * @param obj 发送内容
     */
    public void sendInfo(String topic, Object obj);
}
