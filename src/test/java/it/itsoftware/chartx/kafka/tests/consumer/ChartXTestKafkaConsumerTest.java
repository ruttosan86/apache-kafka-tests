package it.itsoftware.chartx.kafka.tests.consumer;

import static org.junit.Assert.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import org.junit.Test;

import it.itsoftware.chartx.kafka.tests.ChartX;

public class ChartXTestKafkaConsumerTest {

	@Test
	public void test() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9192,localhost:9193,localhost:9194,localhost:9195");
		props.put("group.id", "None");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		ChartXTestKafkaConsumer consumer = new ChartXTestKafkaConsumer(ChartX.TOPIC, props);
		
		consumer.consume(Duration.of(65, ChronoUnit.SECONDS));
		
		
	}

}
