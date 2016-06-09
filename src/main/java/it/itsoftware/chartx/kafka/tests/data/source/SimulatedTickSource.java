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

package it.itsoftware.chartx.kafka.tests.data.source;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

import it.itsoftware.chartx.kafka.tests.data.Tick;

public class SimulatedTickSource implements TickSource {
	
	/**
	 * by Davide Soldi 01/06/2016 11:35 AM
	 */
	
	private static final float MIN_PRC = 10f;
	private static final float MAX_PRC = 20f;
	private static final int MAX_TRADE_NUM = 500;
	private static final float AUTOMATIC_PROBABILITY = 0.8f;
	
	private int nTopics;
	private Random random;
	private long nanos;
	
	public SimulatedTickSource(int nTopics) {
		this.nTopics = nTopics;
		random = new Random(42L);
		nanos = 0;
	}

	@Override
	public Tick next() {
		String contractId = UUID.randomUUID().toString();
		String topic = "TIT." + random.nextInt(nTopics);
		String market = "SIM";
		Float last_prc = newPrice();
		Integer trade_num = newTradeNum();
		Float best_ask = newPrice();
		Long time = Instant.now().toEpochMilli(); //MILLISECOND TIMESTAMP
		time = time * 1000000;
		time += (nanos%1000000);
		nanos++;
		Float best_bid = newPrice();
		Boolean automatic_type = newAutomaticType();
		Tick t = new Tick(topic, market, time, contractId, last_prc, best_bid, best_ask, trade_num, automatic_type);
		return t;
	}
	
	private boolean newAutomaticType() {
		if(random.nextFloat() < AUTOMATIC_PROBABILITY) {
			return true;
		}
		return false;
	}
	
	private int newTradeNum() {
		return 1 + random.nextInt(MAX_TRADE_NUM);
	}
	
	private float newPrice() {
		return MIN_PRC + random.nextFloat() * (MAX_PRC - MIN_PRC);
	}

	@Override
	public boolean hasNext() {
		return true;
	}

	@Override
	public boolean open() {
		return true;
	}

	@Override
	public boolean close() {
		return true;
	}

}
