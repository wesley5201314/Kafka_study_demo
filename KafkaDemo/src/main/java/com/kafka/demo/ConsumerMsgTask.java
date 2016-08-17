package com.kafka.demo;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerMsgTask implements Runnable {

	private KafkaStream<byte[], byte[]> m_stream;
	private int m_threadNumber;

	public ConsumerMsgTask(KafkaStream<byte[], byte[]> stream, int threadNumber) {
		m_threadNumber = threadNumber;
		m_stream = stream;
	}

	public void run() {
		System.out.println("-----Consumers begin to consume-------");
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		while (it.hasNext()){
			System.out.println("Thread " + m_threadNumber + ": "+ new String(it.next().message()));
		}
		System.out.println("Shutting down Thread: " + m_threadNumber);
	}

}
