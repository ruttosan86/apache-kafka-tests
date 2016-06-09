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

package it.itsoftware.chartx.kafka.tests.data;

import java.util.concurrent.TimeUnit;

import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;

public class Tick implements InfluxDBExportable {

	/**
	 * by Davide Soldi 01/06/2016 11:30 AM
	 */

	private String topic;
	private String market;
	private Long time;
	private String contractId;
	private Float last_prc;
	private Float best_bid;
	private Float best_ask;
	private Integer trade_num;
	private Boolean automatic_type;

	public Tick() {
		super();
	}

	public Tick(String topic, String market, Long time, String contractId, Float last_prc, Float best_bid,
			Float best_ask, Integer trade_num, Boolean automatic_type) {
		super();
		this.topic = topic;
		this.market = market;
		this.time = time;
		this.contractId = contractId;
		this.last_prc = last_prc;
		this.best_bid = best_bid;
		this.best_ask = best_ask;
		this.trade_num = trade_num;
		this.automatic_type = automatic_type;
	}

	public String getContractId() {
		return contractId;
	}

	public void setContractId(String contractId) {
		this.contractId = contractId;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public Float getLast_prc() {
		return last_prc;
	}

	public void setLast_prc(Float last_prc) {
		this.last_prc = last_prc;
	}

	public Float getBest_bid() {
		return best_bid;
	}

	public void setBest_bid(Float best_bid) {
		this.best_bid = best_bid;
	}

	public Float getBest_ask() {
		return best_ask;
	}

	public void setBest_ask(Float best_ask) {
		this.best_ask = best_ask;
	}

	public Integer getTrade_num() {
		return trade_num;
	}

	public void setTrade_num(Integer trade_num) {
		this.trade_num = trade_num;
	}

	public Boolean getAutomatic_type() {
		return automatic_type;
	}

	public void setAutomatic_type(Boolean automatic_type) {
		this.automatic_type = automatic_type;
	}

	public String getMarket() {
		return market;
	}

	public void setMarket(String market) {
		this.market = market;
	}

	@Override
	public String toString() {
		return "Tick [topic=" + topic + ", market=" + market + ", time=" + time + ", contractId=" + contractId
				+ ", last_prc=" + last_prc + ", best_bid=" + best_bid + ", best_ask=" + best_ask + ", trade_num="
				+ trade_num + ", automatic_type=" + automatic_type + "]";
	}

	/**
	 * INFLUXDB CONVERSION
	 */
	@Override
	public Point toPoint(String measurement) {
		return toPoint(this, measurement);
	}

	public static Point toPoint(Tick tick, String measurement) {
		Builder builder = Point.measurement(measurement);
		builder.tag("topic", tick.topic);
		builder.tag("market", tick.market);
		builder.time(tick.getTime(), TimeUnit.NANOSECONDS);
		builder.addField("contractId", tick.getContractId());
		builder.addField("last_prc", tick.getLast_prc());
		builder.addField("best_bid", tick.getBest_bid());
		builder.addField("best_ask", tick.getBest_ask());
		builder.addField("trade_num", tick.getTrade_num());
		builder.addField("automatic_type", tick.getAutomatic_type());
		return builder.build();
	}

}
