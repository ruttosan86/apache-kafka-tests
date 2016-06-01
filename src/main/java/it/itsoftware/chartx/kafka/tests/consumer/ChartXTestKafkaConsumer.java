package it.itsoftware.chartx.kafka.tests.consumer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class ChartXTestKafkaConsumer {

	private Properties props;
	private String topic;
	private Gson gson;
	private int maxBufferSize = 1000;

	public ChartXTestKafkaConsumer(String topic, Properties props) {
		this.props = props;
		this.topic = topic;
		gson = new Gson();
	}

	public void setMaxBufferSize(int maxBufferSize) {
		this.maxBufferSize = maxBufferSize;
	}

	public void consume(Duration d) {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {
			private static final long serialVersionUID = 1L;
		};
//		consumer.subscribe(Arrays.asList(topic));
		consumer.assign(Arrays.asList(new TopicPartition(topic, 0)));
		consumer.partitionsFor(topic).forEach(System.out::println);
//		consumer.seekToBeginning(Arrays.asList(new TopicPartition(topic, 0)));
		long n = 0;
		Instant start = Instant.now();
		ArrayList<String> inBuffer = new ArrayList<String>();
		while (true) {
			if(Duration.between(start, Instant.now()).compareTo(d) > 0) {
				System.out.println("TIMEOUT - Ending consumer");
				break;
			}
			ConsumerRecords<String, String> records = consumer.poll(100);
						
			for (ConsumerRecord<String, String> record : records) {
				
				inBuffer.add(record.value());
				Map<String, Object> tags = gson.fromJson(record.value(), typeToken.getType());
				
				Instant ts = Instant.ofEpochMilli(record.timestamp()/1000000);
				System.out.println(++n + ") " + ts.toString() + " <- " + tags.get("topic") + ", " + (Double)tags.get("trade_price"));
			}
			
			if(inBuffer.size() >= maxBufferSize) {
				//dostuff
				inBuffer.clear();
			}
		}
		consumer.close();
		
	}

}
