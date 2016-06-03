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

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONDeserializer<T> implements Deserializer<T> {
	
	final static Logger logger = Logger.getLogger("JSONSerializer");
	private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final Class<T> type;
	
	public JSONDeserializer(Class<T> type) {
		this.type = type;
	}
	
	@Override
	public void close() {
		// Nothing to do
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// Nothing to do
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		try {
			return OBJECT_MAPPER.readValue(data, type);
		} catch (IOException e) {
			logger.severe(String.format("Json processing failed for object: %s", type.getName()));
			logger.severe("EXCEPTION: " + e.getMessage());
		}
		return null;
	}

}
