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

package it.itsoftware.chartx.kafka.tests.data.output;

import static org.junit.Assert.*;

import java.util.logging.Logger;

import org.junit.Test;

import it.itsoftware.chartx.kafka.tests.data.Tick;
import it.itsoftware.chartx.kafka.tests.data.source.SimulatedTickSource;
import it.itsoftware.chartx.kafka.tests.data.source.TickSource;

public class InfluxDBTickOutputTest {

	final static Logger logger = Logger.getLogger("InfluxDBTickOutputTest");

	@Test
	public void test() {
		TickSource source = new SimulatedTickSource(10);
		TickOutput output = new InfluxDBTickOutput("http://localhost:8086", "root", "root", "dbkafkatest", "default",
				"ticks");
		((InfluxDBTickOutput) output).enableBatch(2000);
		assertTrue(output.open());
		logger.info("Tick output opened.");
		assertTrue(source.open());
		logger.info("Tick source opened.");
		for (int i = 0; i < 100; i++) {
			if (source.hasNext()) {
				Tick next = source.next();
				assertNotNull(next);
				output.write(next);
			}
		}
		assertTrue(output.close());
		logger.info("Tick output closed.");
		assertTrue(source.close());
		logger.info("Tick source closed.");
	}

}
