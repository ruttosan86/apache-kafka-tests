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
package it.itsoftware.chartx.kafka.tests.data.serde;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class JSONSerde<T> implements Serde<T> {
	
	private final Serde<T> serde;
	
	public JSONSerde(Class<T> type) {
		JSONSerializer<T> serializer = new JSONSerializer<T>();
		JSONDeserializer<T> deSerializer = new JSONDeserializer<>(type);
		serde = Serdes.serdeFrom(serializer, deSerializer);		
	}

	@Override
	public void close() {
		serde.close();
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		serde.configure(configs, isKey);
	}

	@Override
	public Deserializer<T> deserializer() {
		return serde.deserializer();
	}

	@Override
	public Serializer<T> serializer() {
		// TODO Auto-generated method stub
		return serde.serializer();
	}

}
