package it.itsoftware.chartx.kafka.tests;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.streams.kstream.Windowed;

import it.itsoftware.chartx.kafka.tests.consumer.MyKafkaConsumer;
import it.itsoftware.chartx.kafka.tests.data.TickAggregation;
import it.itsoftware.chartx.kafka.tests.data.output.InfluxDBAggregateOutput;
import it.itsoftware.chartx.kafka.tests.data.serde.TickAggregationJSONDeserializer;
import it.itsoftware.chartx.kafka.tests.data.serde.WindowedStringDeserializer;

public class AggregateKafkaToInfluxMain {

	final static Logger logger = Logger.getLogger("AggregateKafkaToInfluxMain");

	public static void main(String[] args) {
		String topic = "aggregateTicks";
		final AtomicBoolean closed = new AtomicBoolean(false);
		InfluxDBAggregateOutput output = new InfluxDBAggregateOutput("http://localhost:8086", "root", "root",
				"dbkafkatest", "default", "ticks_min");
		output.enableBatch(2000);

		output.open();
		ConcurrentHashMap<Long, ConcurrentHashMap<String, TickAggregation>> bucket = new ConcurrentHashMap<Long, ConcurrentHashMap<String, TickAggregation>>();
		Properties props = MyKafkaConsumer.defaultProperties(WindowedStringDeserializer.class,
				TickAggregationJSONDeserializer.class);

		Long tmp = getCurrentMinute();
		tmp -= (60 * 1000);
		AtomicLong queryMinute = new AtomicLong(tmp);

		Runnable aggregateConsumerThread = () -> {
			KafkaConsumer<Windowed<String>, TickAggregation> consumer = new KafkaConsumer<>(props);
			int i=0;
			try {
				consumer.subscribe(Arrays.asList(topic));
				while (!closed.get()) {
					ConsumerRecords<Windowed<String>, TickAggregation> records = consumer.poll(250);
					if((++i%250) == 0){
						 logger.info("Received " + records.count() + " aggregate updates");
						 i=0;
					}
					for (ConsumerRecord<Windowed<String>, TickAggregation> record : records) {
						Long key = new Long(record.key().window().start());
						if (!bucket.containsKey(key)) {
							logger.info("New KEY: " + key.toString());
							bucket.put(key, new ConcurrentHashMap<String, TickAggregation>());
						}
						bucket.get(key).put(record.value().getTopic(), record.value());
					}
				}
			} catch (WakeupException e) {
				// Ignore exception if closing
				if (!closed.get())
					throw e;
			} finally {
				output.close();
				consumer.close();
			}
		};

		Runnable influxDbOutThread = () -> {
			logger.info("Working on updating influx!");
			Long time = queryMinute.get();
			System.out.println(Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()));
			ConcurrentHashMap<String, TickAggregation> window = bucket.get(time);
			if (window != null) {
				logger.info("Writing " + window.size() + " aggregates");
				window.forEach((k, v) -> {
					v.setTime(time);
					output.write(v);
					System.out.println(v.toString());
				});
				bucket.remove(time);
			}
			queryMinute.addAndGet(60L * 1000L);
		};

		new Thread(aggregateConsumerThread).start();

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(influxDbOutThread, 0, 60, TimeUnit.SECONDS);
		
		

	}

	private static Long getCurrentMinute() {
		Long currentMinute = Instant.now().toEpochMilli();
		long tmp = currentMinute % (60 * 1000);
		currentMinute -= tmp;
		return currentMinute;
	}

}
