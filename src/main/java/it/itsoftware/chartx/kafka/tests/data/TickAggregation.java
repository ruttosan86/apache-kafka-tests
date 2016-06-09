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

public class TickAggregation implements InfluxDBExportable {

	private Long time;
	private Float min_prc;
	private Float max_prc;
	private Float in_prc;
	private Float out_prc;
	private Integer num_trades;
	private Integer contract_count;
	private String topic;

	public TickAggregation() {
		super();
		time = 0L;
		min_prc = Float.MAX_VALUE;
		max_prc = Float.MIN_VALUE;
		in_prc = null;
		out_prc = null;
		num_trades = 0;
		contract_count = 0;
	}

	public TickAggregation add(Tick tick) {
		if (topic == null) {
			topic = tick.getTopic() + "." + tick.getMarket();
			in_prc = tick.getLast_prc();
		}
		contract_count++;
		num_trades += tick.getTrade_num();
		if (tick.getLast_prc() > max_prc) {
			max_prc = tick.getLast_prc();
		}
		if (tick.getLast_prc() < min_prc) {
			min_prc = tick.getLast_prc();
		}
		out_prc = tick.getLast_prc();
		return this;
	}

	public Float getMin_prc() {
		return min_prc;
	}

	public void setMin_prc(Float min_prc) {
		this.min_prc = min_prc;
	}

	public Float getMax_prc() {
		return max_prc;
	}

	public void setMax_prc(Float max_prc) {
		this.max_prc = max_prc;
	}

	public Integer getNum_trades() {
		return num_trades;
	}

	public void setNum_trades(Integer num_trades) {
		this.num_trades = num_trades;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Integer getContract_count() {
		return contract_count;
	}

	public void setContract_count(Integer contract_count) {
		this.contract_count = contract_count;
	}

	public Float getIn_prc() {
		return in_prc;
	}

	public void setIn_prc(Float in_prc) {
		this.in_prc = in_prc;
	}

	public Float getOut_prc() {
		return out_prc;
	}

	public void setOut_prc(Float out_prc) {
		this.out_prc = out_prc;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	@Override
	public Point toPoint(String measurement) {
		return toPoint(this, measurement);
	}

	public static Point toPoint(TickAggregation tick, String measurement) {
		Builder builder = Point.measurement(measurement);
		builder.tag("topic", tick.topic);
		builder.time(tick.getTime(), TimeUnit.MILLISECONDS);
		builder.addField("min_prc", tick.min_prc);
		builder.addField("max_prc", tick.max_prc);
		builder.addField("in_prc", tick.in_prc);
		builder.addField("out_prc", tick.out_prc);
		builder.addField("num_trades", tick.num_trades);
		builder.addField("contract_count", tick.contract_count);
		return builder.build();
	}

	@Override
	public String toString() {
		return "TickAggregation [time=" + time + ", min_prc=" + min_prc + ", max_prc=" + max_prc + ", in_prc=" + in_prc
				+ ", out_prc=" + out_prc + ", num_trades=" + num_trades + ", contract_count=" + contract_count
				+ ", topic=" + topic + "]";
	}

}
