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


package it.itsoftware.chartx.kafka.tests.data.old;

import static org.junit.Assert.*;

import java.util.logging.Logger;

import org.junit.Test;

import it.itsoftware.chartx.kafka.tests.data.Tick;
import it.itsoftware.chartx.kafka.tests.data.source.SimulatedTickSource;
import it.itsoftware.chartx.kafka.tests.data.source.TickSource;

public class SimulatedTickDataSourceTest {

	final static Logger logger = Logger.getLogger("SimulatedTickDataSourceTest");
	
	@Test
	public void test() {
		TickSource source = new SimulatedTickSource(10);
		if(source.open()){
			for(int i=0; i<100; i++) {
				if(source.hasNext()) {
					Tick next = source.next();
					assertNotNull(next);
					logger.info(next.toString());
				}
			}
			if(source.close()) {
				logger.info("Tick source closed.");
			} else {
				logger.severe("Failed to close source");
				fail();
			}
		} else {
			logger.severe("Failed to open source");
			fail();
		}
		
	}

}
