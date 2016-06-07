/*
 * Copyright 2016 Davide Soldi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.itsoftware.chartx.kafka.tests;

import java.util.Properties;

import org.apache.kafka.streams.kstream.Windowed;

import it.itsoftware.chartx.kafka.tests.consumer.MyKafkaConsumer;
import it.itsoftware.chartx.kafka.tests.data.TickAggregation;
import it.itsoftware.chartx.kafka.tests.data.output.ConsoleOutput;
import it.itsoftware.chartx.kafka.tests.data.output.Output;
import it.itsoftware.chartx.kafka.tests.data.serde.TickAggregationJSONDeserializer;
import it.itsoftware.chartx.kafka.tests.data.serde.WindowedStringDeserializer;

public class ConsumerMain {

	public static void main(String[] args) throws InterruptedException {
		String topic = "aggregateTicks";
//		boolean influx = false;
		Output<Windowed<String>, TickAggregation> output = null;
//		if(influx) {
//			output = new InfluxDBTickOutput("http://localhost:8086", "root", "root", "dbkafkatest", "default",
//					"ticks");
//			((InfluxDBTickOutput) output).enableBatch(2000);
//		} else {
			output = new ConsoleOutput<Windowed<String>, TickAggregation>();
//		}
		if(output.open()) {
			
			Properties props = MyKafkaConsumer.defaultProperties(WindowedStringDeserializer.class, TickAggregationJSONDeserializer.class);
			
			MyKafkaConsumer<Windowed<String>, TickAggregation> consumer = new MyKafkaConsumer<Windowed<String>, TickAggregation>(props, topic, output);
			if(consumer.start()) {
				Thread.sleep(150000);
				consumer.stop();
			}
			
		}
	}

}
