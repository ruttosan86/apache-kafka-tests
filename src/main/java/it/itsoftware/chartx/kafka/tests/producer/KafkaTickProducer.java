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

package it.itsoftware.chartx.kafka.tests.producer;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import it.itsoftware.chartx.kafka.tests.data.Tick;
import it.itsoftware.chartx.kafka.tests.data.source.TickSource;

public class KafkaTickProducer {

	private TickSource source;
	private String destinationTopic;
	private Properties props;
	private Producer<String, Tick> producer;
	private ArrayBlockingQueue<Tick> rejectedTicks;
	private boolean productionAborted;

	private static final int MAX_QUEUE_SIZE = 32768;

	final static Logger logger = Logger.getLogger("KafkaTickProducer");

	public KafkaTickProducer(TickSource source, String destinationTopic, Properties props) {
		super();
		this.source = source;
		this.destinationTopic = destinationTopic;
		this.props = props;
		rejectedTicks = new ArrayBlockingQueue<Tick>(MAX_QUEUE_SIZE);
		producer = null;
	}

	public void produceAsync(int nMessages) {
		open();
		productionAborted = false;
		for (int i = 0; i < nMessages; i++) {
			if (productionAborted) {
				return;
			}
			Tick tick = source.next();
			sendAsync(tick);
		}
		if (!rejectedTicks.isEmpty()) {
			rejectedTicks.forEach(this::sendAsync);
		}
	}
	
	public void produceSync(int nMessages) {
		open();
		productionAborted = false;
		for (int i = 0; i < nMessages; i++) {
			if (productionAborted) {
				return;
			}
			Tick tick = source.next();
			sendSync(tick);
		}
		if (!rejectedTicks.isEmpty()) {
			rejectedTicks.forEach(this::sendSync);
		}
	}

	private void sendAsync(Tick tick) {
		ProducerRecord<String, Tick> record = new ProducerRecord<String, Tick>(destinationTopic, tick.getTopic(),
				tick);
		producer.send(record, (meta, error) -> {
			if (error != null) {
				logger.severe("Unable to send tick:\n" + error.getMessage());
				if (!productionAborted) // do not put the same tick twice in
										// queue
					if (!rejectedTicks.offer(tick)) {
						logger.severe("Queue full! Stopping produce loop.");
						productionAborted = true;
					}
			}
		});
	}

	private void sendSync(Tick tick) {
		ProducerRecord<String, Tick> record = new ProducerRecord<String, Tick>(destinationTopic, tick.getTopic(),
				tick);
		Future<RecordMetadata> future = producer.send(record);
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			logger.severe("Unable to send tick:\n" + e.getMessage());
			if (!productionAborted) // do not put the same tick twice in queue
				if (!rejectedTicks.offer(tick)) {
					logger.severe("Queue full! Stopping produce loop.");
					productionAborted = true;
				}
		}

	}

	private void open() {
		if (producer == null) {
			producer = new KafkaProducer<String, Tick>(props);
		}
	}

	public void close() {
		if (producer != null) {
			producer.close();
			producer = null;
		}
	}

	public static Properties defaultProperties() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9192");
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "it.itsoftware.chartx.kafka.tests.data.serde.TickJSONSerializer");
		return props;
	}

}
