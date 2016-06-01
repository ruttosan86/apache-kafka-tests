package it.itsoftware.chartx.kafka.tests.producer;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;

import it.itsoftware.chartx.kafka.tests.ChartX;
import it.itsoftware.chartx.kafka.tests.data.FileTickDataSource;
import it.itsoftware.chartx.kafka.tests.data.TickDataFactory;

public class ChartXTestKafkaProducerTest {

	@Test
	public void test() {
		System.out.println("New datasource");
		FileTickDataSource datasource = new FileTickDataSource();
		System.out.println("Setting time simulation");
		datasource.simulateTime();
		System.out.println("Setting topic simulation");
		datasource.simulateTopics(1000);
		TickDataFactory factory = new TickDataFactory(datasource);
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9192,localhost:9193,localhost:9194,localhost:9195");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		ChartXTestKafkaProducer producer = new ChartXTestKafkaProducer(factory, props, ChartX.TOPIC);
		try {
			for(int i=0; i<100; i++) {
				producer.produce(300);
				Thread.sleep(500);
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

	}

}
