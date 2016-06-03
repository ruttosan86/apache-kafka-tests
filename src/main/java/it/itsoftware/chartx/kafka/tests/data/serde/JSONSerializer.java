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
import java.util.logging.Logger;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONSerializer<T> implements Serializer<T> {

	final static Logger logger = Logger.getLogger("JSONSerializer");
	private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	
	@Override
	public void close() {
		// Nothing to do
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// Nothing to do
	}

	@Override
	public byte[] serialize(String topic, T data) {
		try {
			return OBJECT_MAPPER.writeValueAsBytes(data);
		} catch (Exception e) {
			logger.severe(String.format("Json processing failed for object: %s", data.getClass().getName()));
			logger.severe("EXCEPTION: " + e.getMessage());
		}
		return "".getBytes();
	}

}
