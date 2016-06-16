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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.kstream.Windowed;

import it.itsoftware.chartx.kafka.tests.consumer.MyKafkaConsumer;
import it.itsoftware.chartx.kafka.tests.data.TickAggregation;
import it.itsoftware.chartx.kafka.tests.data.serde.TickAggregationJSONDeserializer;
import it.itsoftware.chartx.kafka.tests.data.serde.WindowedStringDeserializer;

public class ZeroOffsetConsumer {

	public static void main(String[] args) throws IOException {
		Properties props = MyKafkaConsumer.defaultProperties(WindowedStringDeserializer.class, TickAggregationJSONDeserializer.class);
		
		KafkaConsumer<Windowed<String>, TickAggregation> consumer = new KafkaConsumer<>(props);
		List<PartitionInfo> partitions = consumer.listTopics().get("aggregateTicks");
		ArrayList<TopicPartition> toBeAssigned = new ArrayList<TopicPartition>();
		partitions.forEach(p -> {
				System.out.println(p.toString());
				toBeAssigned.add(new TopicPartition(p.topic(), p.partition()));
		});
		
		consumer.assign(toBeAssigned);
		consumer.seekToBeginning(toBeAssigned);
//		consumer.seekToEnd(Collections.singletonList(toBeAssigned.get(0)));
		ConsumerRecords<Windowed<String>, TickAggregation> records = consumer.poll(250);
		int i=0;
    	for (ConsumerRecord<Windowed<String>, TickAggregation> record : records) {
    		if(record.value() != null) {
    			record.value().setTime(record.key().window().start());
    			System.out.println(record.key().toString() + " - " + record.value());
    		}
    		if(++i %100 ==0) {
    			System.in.read();
    		}
    	}
    	System.out.println("Retrieved " + records.count() + " ticks.");
		consumer.close();
	}

}
