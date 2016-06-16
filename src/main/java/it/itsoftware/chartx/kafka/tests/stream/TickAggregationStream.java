package it.itsoftware.chartx.kafka.tests.stream;

import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import com.google.common.base.Stopwatch;

import it.itsoftware.chartx.kafka.tests.data.Tick;
import it.itsoftware.chartx.kafka.tests.data.TickAggregation;
import it.itsoftware.chartx.kafka.tests.data.serde.JSONSerde;
import it.itsoftware.chartx.kafka.tests.data.serde.TickJSONSerde;
import it.itsoftware.chartx.kafka.tests.data.serde.WindowedStringSerde;

public class TickAggregationStream {

	private KafkaStreams streams;

	public TickAggregationStream(Properties props) {
		this(props, "ticks", "aggregateTicks");
	}
	
	public TickAggregationStream(Properties props, String tickTopic, String aggregateTopic) {
		final Serde<String> stringSerde = Serdes.String();
		final TickJSONSerde tickSerde = new TickJSONSerde();
		final JSONSerde<TickAggregation> aggregateSerde = new JSONSerde<TickAggregation>(TickAggregation.class);
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, Tick> ticks = builder.stream(stringSerde, tickSerde, tickTopic);
		KTable<Windowed<String>, TickAggregation> aggregates = ticks.aggregateByKey(TickAggregation::new,
				(topic, tick, aggregate) -> aggregate.add(tick), TimeWindows.of("minute_window", 1L * 60L * 1000L * 1000000L),
				stringSerde, aggregateSerde);
		aggregates.toStream().to(new WindowedStringSerde(), aggregateSerde, aggregateTopic);	
		 
		
		
		streams = new KafkaStreams(builder, props);
	}

	public synchronized boolean start() {
		if (streams != null) {
			streams.start();
			return true;
		}
		return false;
	}

	public synchronized boolean close() {
		if(streams!=null){
			streams.close();
			return true;
		}
		return false;
	}

	public static <T> Properties defaultProperties(Class<T> timestampExtractorClass) {
		return defaultProperties(timestampExtractorClass.getCanonicalName());
	}

	public static Properties defaultProperties(String timestampExtractorClass) {
		Properties props = new Properties();
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "tick-minute-aggregation-stream");
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tick-aggregator-v-0.0.1");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9192");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, TickJSONSerde.class.getName());
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, timestampExtractorClass);
		return props;
	}

	public static Properties defaultProperties() {
		return defaultProperties("org.apache.kafka.streams.processor.ConsumerRecordTimestampExtractor");
	}

}
