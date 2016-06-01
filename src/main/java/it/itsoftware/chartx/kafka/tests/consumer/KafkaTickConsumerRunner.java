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

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import com.google.gson.Gson;

import it.itsoftware.chartx.kafka.tests.data.Tick;
import it.itsoftware.chartx.kafka.tests.data.output.TickOutput;

public class KafkaTickConsumerRunner extends Thread {

	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final KafkaConsumer<String, String> consumer;
	private TickOutput output;
	private Gson gson;
	private String sourceTopic;
	
	final static Logger logger = Logger.getLogger("KafkaTickConsumerRunner");
	
	public KafkaTickConsumerRunner(Properties props, TickOutput output, String sourceTopic) {
		super();
		this.consumer = new KafkaConsumer<String, String>(props);
		this.output = output;
		this.sourceTopic = sourceTopic;
		this.gson = new Gson();
	}

	@Override
	public void run() {
		try {
			logger.info("Starting consumer.");
            consumer.subscribe(Arrays.asList(sourceTopic));
            output.open();
            while (!closed.get()) {
            	ConsumerRecords<String, String> records = consumer.poll(250);
            	logger.info("Retrieved " + records.count() + " ticks.");
            	for (ConsumerRecord<String, String> record : records) {
            		Tick tick = gson.fromJson(record.value(), Tick.class);
            		if(tick != null) {
            			output.write(tick);
            		}
            	}
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
        	logger.info("Closing consumer and output..");
        	output.close();
            consumer.close();
            logger.info("Closing done.");
        }

	}

	public void shutdown() {
		logger.info("Shutdown requested.");
		closed.set(true);
		consumer.wakeup();
	}

}
