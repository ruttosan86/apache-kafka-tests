package it.itsoftware.chartx.kafka.tests.producer;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;

import it.itsoftware.chartx.kafka.tests.data.TickDataSource;

public class ChartXTestKafkaProducer {

	private TickDataSource datasource;
	private Properties props;
	private String topic;
	private Gson gson;

	public ChartXTestKafkaProducer(TickDataSource datasource, Properties producerProperties, String topic) {
		super();
		props = producerProperties;
		this.datasource = datasource;
		this.topic = topic;
		gson = new Gson();
	}

	public void produce(int nMessages) throws Exception {
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		for (int i = 0; i < nMessages; i++) {
			Map<String, Object> record = datasource.nextTick();
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, i % 3,
					(Long) record.get("time"), (String) record.get("topic"), gson.toJson(record));
			producer.send(data, (meta, e) -> {
				if (e != null) {
					e.printStackTrace();
				} else {
					System.out.println(meta.toString());
				}
			});
		}
		producer.close();
	}
	
	

}
