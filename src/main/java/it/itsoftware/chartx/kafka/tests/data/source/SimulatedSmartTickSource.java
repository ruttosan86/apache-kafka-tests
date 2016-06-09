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
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Logger;

import it.itsoftware.chartx.kafka.tests.data.Tick;

public class SimulatedSmartTickSource implements TickSource {

	final static Logger logger = Logger.getLogger("SimulatedSmartTickSource");
	
	private static final float MIN_PRC = 10f;
	private static final float MAX_PRC = 20f;
	private static final int MAX_TRADE_NUM = 500;
	private static final float AUTOMATIC_PROBABILITY = 0.8f;
	
	private ArrayList<Serie> series;

	private boolean randomSerieSelection;
	
	private int nextTickIndex;
	private long nanos;
	
	private Random random;

	public SimulatedSmartTickSource(int nSeries, int nPotentialSeries, int nTopics) {
		if (nSeries <= 0 || nPotentialSeries <= 0 || nTopics <= 0)
			throw new IllegalArgumentException("Only positive non zero parameters are allowed.");
		if (nSeries > nPotentialSeries)
			throw new IllegalArgumentException("nSeries must be less or equal to nPotentialSeries");
		if (nTopics > nSeries)
			throw new IllegalArgumentException("nTopics must be less or equal to nSeries");
		
		int nMarkets = nPotentialSeries / nTopics;
		int marketsPerTopic = nSeries / nTopics;
		System.out.println("marketsPerTopic " + marketsPerTopic);
		System.out.println("nMarkets " + nMarkets);
		System.out.println("mul " + (marketsPerTopic * nTopics));
		if((marketsPerTopic * nTopics) < nMarkets) {
			logger.warning("The potential number of markets (" + nMarkets + ") will never be reached. The maximum is " + (marketsPerTopic * nTopics));
		}
		
		nextTickIndex = 0;
		
		series = new ArrayList<Serie>();
		int numTopicChars = String.valueOf(nTopics).length();
		int numMarketChars = String.valueOf(nMarkets).length();
		int marketIndex = 0;
		for(int i=0; i<nTopics; i++) {
			String topic = "TIT." + String.format("%0" + numTopicChars + "d", i);
			for(int j=0; j<marketsPerTopic; j++) {
				int m = marketIndex % nMarkets;
				String market = "MAR" + String.format("%0" + numMarketChars + "d", m);
				series.add(new Serie(topic, market));
				marketIndex++;
			}
		}
		nextTickIndex = 0;
		randomSerieSelection = false;
		random = new Random(42L);
		nanos = 0;
	}

	@Override
	public Tick next() {
		Serie s = null;
		if(randomSerieSelection) {
			s = randomSerie();
		} else {
			s = nextSerie();
		}
		String contractId = UUID.randomUUID().toString();
		Float last_prc = newPrice();
		Integer trade_num = newTradeNum();
		Float best_ask = newPrice();
		Long time = Instant.now().toEpochMilli(); //NANOSECOND TIMESTAMP
		time = time * 1000000;
		time += (nanos%1000000);
		nanos++;
		Float best_bid = newPrice();
		Boolean automatic_type = newAutomaticType();
		Tick t = new Tick(s.topic, s.market, time, contractId, last_prc, best_bid, best_ask, trade_num, automatic_type);
		return t;
	}
	
	private Serie nextSerie() {
		if(nextTickIndex >= series.size()) 
			nextTickIndex = 0;
		return series.get(nextTickIndex++);
	}
	
	private Serie randomSerie() {
		return series.get(random.nextInt(series.size()));
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
	
	public void randomize() {
		randomSerieSelection = true;
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
	
	public static class Serie {

		private String topic;
		private String market;

		public Serie(String topic, String market) {
			super();
			this.topic = topic;
			this.market = market;
		}

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		public String getMarket() {
			return market;
		}

		public void setMarket(String market) {
			this.market = market;
		}

		@Override
		public String toString() {
			return "Serie [topic=" + topic + ", market=" + market + "]";
		}
		
	}

	public ArrayList<Serie> getSeries() {
		return series;
	}

	public void setSeries(ArrayList<Serie> series) {
		this.series = series;
	}

}
