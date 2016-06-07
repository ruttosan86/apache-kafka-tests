package it.itsoftware.chartx.kafka.tests;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import it.itsoftware.chartx.kafka.tests.consumer.MyKafkaConsumer;
import it.itsoftware.chartx.kafka.tests.data.Tick;
import it.itsoftware.chartx.kafka.tests.data.output.InfluxDBOutput;
import it.itsoftware.chartx.kafka.tests.data.output.Output;
import it.itsoftware.chartx.kafka.tests.data.serde.TickJSONDeserializer;

public class TickKafkaToInfluxMain {

	public static void main(String[] args) throws InterruptedException {
		String topic = "ticks";
		
		Output<String, Tick> output = new InfluxDBOutput<String, Tick>("http://localhost:8086", "root", "root", "dbkafkatest", "default",
				"ticks");
		((InfluxDBOutput<String, Tick>) output).enableBatch(10000);

		if(output.open()) {
			Properties props = MyKafkaConsumer.defaultProperties(StringDeserializer.class, TickJSONDeserializer.class);
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "tickToInfluxConsumerGroup");
			MyKafkaConsumer<String, Tick> consumer = new MyKafkaConsumer<String, Tick>(props, topic, output);
			if(consumer.start()) {
				Thread.sleep(15L * 60L * 1000L);
				consumer.stop();
			}
			
		}
		
	}

}
