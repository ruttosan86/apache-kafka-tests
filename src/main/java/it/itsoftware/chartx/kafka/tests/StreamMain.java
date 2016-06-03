package it.itsoftware.chartx.kafka.tests;

import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;

public class StreamMain {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "first-stream-test");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9192");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
		
	}

}
