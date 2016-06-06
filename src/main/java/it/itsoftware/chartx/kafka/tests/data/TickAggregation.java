package it.itsoftware.chartx.kafka.tests.data;

public class TickAggregation {

	private Float min_prc;
	private Float max_prc;
	private Float in_prc;
	private long minTs;
	private Float out_prc;
	private long maxTs;
	private Integer num_trades;
	private Integer contract_count;
	private String topic;

	public TickAggregation() {
		super();
		min_prc = Float.MAX_VALUE;
		max_prc = Float.MIN_VALUE;
		in_prc = null;
		out_prc = null;
		minTs = Long.MAX_VALUE;
		maxTs = Long.MIN_VALUE;
		num_trades = 0;
		contract_count = 0;
	}

	public TickAggregation add(Tick tick) {
		if (topic == null) {
			topic = tick.getTopic();
		}
		contract_count++;
		num_trades += tick.getTrade_num();
		if (tick.getLast_prc() > max_prc) {
			max_prc = tick.getLast_prc();
		}
		if (tick.getLast_prc() < min_prc) {
			min_prc = tick.getLast_prc();
		}
		long t = tick.getTime();
		if(t > maxTs) {
			out_prc = tick.getLast_prc();
			maxTs = t;
		}
		if(t < minTs) {
			in_prc = tick.getLast_prc();
			minTs = t;
		}
		return this;
	}

	@Override
	public String toString() {
		return "TickAggregation [min_prc=" + min_prc + ", max_prc=" + max_prc + ", in_prc=" + in_prc + ", out_prc="
				+ out_prc + ", num_trades=" + num_trades + ", contract_count=" + contract_count + ", topic=" + topic
				+ "]";
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

}
