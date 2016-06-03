package it.itsoftware.chartx.kafka.tests.data.time;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import it.itsoftware.chartx.kafka.tests.data.Tick;

public class TickTimestampExtractor implements TimestampExtractor {

	@Override
	public long extract(ConsumerRecord<Object, Object> record) {
		Double millis = ((Tick)record.value()).getTime().doubleValue() / 1000000d;
		return millis.longValue(); //milliseconds
	}

}
