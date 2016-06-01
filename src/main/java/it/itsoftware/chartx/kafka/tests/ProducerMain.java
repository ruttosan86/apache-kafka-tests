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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.base.Stopwatch;

import it.itsoftware.chartx.kafka.tests.data.source.SimulatedTickSource;
import it.itsoftware.chartx.kafka.tests.data.source.TickSource;
import it.itsoftware.chartx.kafka.tests.producer.KafkaTickProducer;

public class ProducerMain {

	final static Logger logger = Logger.getLogger("ProducerMain");

	public static void main(String[] args) {

		String topic = "ticks";
		TickSource source = new SimulatedTickSource(100);
		KafkaTickProducer producer = new KafkaTickProducer(source, topic, KafkaTickProducer.defaultProperties());
		int nMessages = 100;
		boolean asyncSend = true;
		int executionTime = 60; // time in seconds for data production

		Stopwatch exitTimer = Stopwatch.createStarted();
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		Runnable writeProcess = () -> {
			if (exitTimer.elapsed(TimeUnit.SECONDS) > executionTime) {
				logger.info("Timeout reached, shutting down producer");
				producer.close();
				executor.shutdown();
				return;
			}
			logger.info("Producing " + nMessages + " messages.");
			Stopwatch timer = Stopwatch.createStarted();
			if(asyncSend){
				producer.produceAsync(nMessages);
			} else {
				producer.produceSync(nMessages);
			}
			logger.info("Sent in " + (timer.elapsed(TimeUnit.MICROSECONDS) / 1000.0d) + "ms");
		};

		int initialDelay = 0;
		int period = 1000;
		executor.scheduleAtFixedRate(writeProcess, initialDelay, period, TimeUnit.MILLISECONDS);

	}

}
