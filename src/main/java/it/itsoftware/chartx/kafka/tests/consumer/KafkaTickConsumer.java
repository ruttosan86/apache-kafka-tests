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
package it.itsoftware.chartx.kafka.tests.consumer;

import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import it.itsoftware.chartx.kafka.tests.data.output.TickOutput;

public class KafkaTickConsumer {
	
	final static Logger logger = Logger.getLogger("KafkaTickConsumer");
	
	private Properties props;
	private String sourceTopic;
	private TickOutput output;
	private KafkaTickConsumerRunner consumer;
	private boolean started;
	
	public KafkaTickConsumer(Properties props, String sourceTopic, TickOutput output) {
		super();
		this.props = props;
		this.sourceTopic = sourceTopic;
		this.output = output;
		this.started = false;
	}

	public boolean start() {
		logger.info("Start request received.");
		if(started) {
			return false;
		}
		consumer = new KafkaTickConsumerRunner(props, output, sourceTopic);
		consumer.start();
		started = true;
		return true;
	}
	
	public boolean stop() {
		logger.info("Stop request received.");
		if(!started) {
			return false;
		}
		consumer.shutdown();
		try {
			consumer.join(30000);
			started = false;
			consumer = null;
			return true;
		} catch (InterruptedException e) {
			logger.severe("Failed to stop the consumer. - " + e.getMessage());
			return false;
		}
	}
	
	
	public static Properties defaultProperties() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9192");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "chartXconsumer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "it.itsoftware.chartx.kafka.tests.data.serde.TickJSONDeserializer");
		return props;
	}

}
