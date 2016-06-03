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

import it.itsoftware.chartx.kafka.tests.consumer.KafkaTickConsumer;
import it.itsoftware.chartx.kafka.tests.data.output.ConsoleTickOutput;
import it.itsoftware.chartx.kafka.tests.data.output.InfluxDBTickOutput;
import it.itsoftware.chartx.kafka.tests.data.output.TickOutput;

public class ConsumerMain {

	public static void main(String[] args) throws InterruptedException {
		String topic = "ticks2";
		boolean influx = false;
		TickOutput output = null;
		if(influx) {
			output = new InfluxDBTickOutput("http://localhost:8086", "root", "root", "dbkafkatest", "default",
					"ticks");
			((InfluxDBTickOutput) output).enableBatch(2000);
		} else {
			output = new ConsoleTickOutput();
		}
		if(output.open()) {
			KafkaTickConsumer consumer = new KafkaTickConsumer(KafkaTickConsumer.defaultProperties(), topic, output);
			if(consumer.start()) {
				Thread.sleep(150000);
				consumer.stop();
			}
			
		}
	}

}
