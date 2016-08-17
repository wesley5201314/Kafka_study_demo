package com.kafka.demo;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @see https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 * @see http://kafka.apache.org/documentation.html#producerapi
 * @author wesley
 *
 */
public class ProducerDemo {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		int events = 20;
		// @see http://kafka.apache.org/08/configuration.html-- 3.3 Producer
		// Configs
		// @see http://kafka.apache.org/documentation.html#producerconfigs
		// 设置配置属性
		Properties props = new Properties();
		props.put("metadata.broker.list", "127.0.0.1:9092"); // 配置kafka的IP和端口
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// key.serializer.class默认为serializer.class
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		// 可选配置，如果不配置，则使用默认的partitioner
		props.put("partitioner.class", "com.kafka.demo.PartitionerDemo");
		// 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
		// 值为0,1,-1,可以参考
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);

		// 创建producer
		Producer<String, String> producer = new Producer<String, String>(config);
		// 产生并发送消息
		long start = System.currentTimeMillis();
		for (long i = 0; i < events; i++) {
			long runtime = new Date().getTime();
			String ip = "192.168.1." + i;
			String msg = runtime + "--www.kafkademo.com--" + ip;
			// 如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
			KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
			System.out.println("-----Kafka Producer----createMessage----" + data);
			producer.send(data);
		}
		System.out.println("Time consuming:" + (System.currentTimeMillis() - start));
		// 关闭producer
		producer.close();
	}
}
