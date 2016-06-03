package it.itsoftware.chartx.kafka.tests;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import it.itsoftware.chartx.kafka.tests.data.Tick;
import it.itsoftware.chartx.kafka.tests.data.TickAggregation;
import it.itsoftware.chartx.kafka.tests.data.serde.JSONSerde;
import it.itsoftware.chartx.kafka.tests.data.serde.TickJSONSerde;

public class StreamMain {

	public static void main(String[] args) throws InterruptedException {
		
		Properties props = new Properties();
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "first-stream-test-client");
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "first-stream");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9192");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, TickJSONSerde.class.getName());
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "it.itsoftware.chartx.kafka.tests.data.time.TickTimestampExtractor");
		
		final Serde<String> stringSerde = Serdes.String();
		final TickJSONSerde tickSerde = new TickJSONSerde();
		
		KStreamBuilder builder = new KStreamBuilder();
		
		
		
		KStream<String, Tick> ticks = builder.stream(stringSerde, tickSerde, "ticks2");
		
//		ticks.filter((k, tick) -> tick.getAutomatic_type().booleanValue()).countByKey(TimeWindows.of("minute", 60)).print();
		
		TimeWindows win = TimeWindows.of("test", 60000);		
		ticks.aggregateByKey(TickAggregation::new, (topic, tick, aggregate) -> aggregate.add(tick), win.advanceBy(60000), stringSerde, new JSONSerde<TickAggregation>(TickAggregation.class)).print();
		
		
        KafkaStreams streams = new KafkaStreams(builder, props);
		
        streams.start();
        
        Thread.sleep(120000);
        
        streams.close();
        
	}

}
