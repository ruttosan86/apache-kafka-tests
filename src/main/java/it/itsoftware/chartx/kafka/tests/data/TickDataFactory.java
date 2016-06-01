package it.itsoftware.chartx.kafka.tests.data;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class TickDataFactory implements TickDataSource {

	private TickDataSource datasource;
	private Schema schema;
	
	public TickDataFactory(TickDataSource datasource) {
		this.datasource = datasource;
		Schema.Parser parser = new Schema.Parser();
		schema = parser.parse(buildTickAvroSchema());
	}
	
	private String buildTickAvroSchema() {
		StringBuffer buffer = new StringBuffer("{\"namespace\": \"tick.avro\", \"type\": \"record\", ");
		buffer.append("\"name\": \"ticks\"," );
		buffer.append("\"fields\": [" );
		buffer.append("{\"name\": \"topic\", \"type\": \"string\"}," );
		buffer.append("{\"name\": \"time\", \"type\": \"long\"}," );
		buffer.append("{\"name\": \"trade_num\", \"type\": \"int\", \"default\": 0}," );
		buffer.append("{\"name\": \"best_bid\", \"type\": \"float\", \"default\": 0}," );
		buffer.append("{\"name\": \"best_ask\", \"type\": \"float\", \"default\": 0}," );
		buffer.append("{\"name\": \"trend_lat\", \"type\": \"int\", \"default\": 0}," );
		buffer.append("{\"name\": \"trend_brit\", \"type\": \"int\", \"default\": 0}," );
		buffer.append("{\"name\": \"last_id\", \"type\": \"string\", \"default\": \"null\"}," );
		buffer.append("{\"name\": \"last_type\", \"type\": \"string\", \"default\": \"null\"}," );
		buffer.append("{\"name\": \"automatic_type\", \"type\": \"string\", \"default\": \"null\"}," );
		buffer.append("{\"name\": \"trade_sign\", \"type\": \"string\", \"default\": \"null\"}," );
		buffer.append("{\"name\": \"trade_price\", \"type\": \"float\", \"default\": 0}," );
		buffer.append("{\"name\": \"last_date\", \"type\": \"long\", \"default\": 0}," );
		buffer.append("{\"name\": \"special_trade\", \"type\": \"string\", \"default\": \"null\"}," );
		buffer.append("{\"name\": \"time_type\", \"type\": \"string\", \"default\": \"null\"}," );
		buffer.append("{\"name\": \"spike_flag\", \"type\": \"int\", \"default\": 0}," );
		buffer.append("{\"name\": \"trade_date\", \"type\": \"long\", \"default\": 0}");
		buffer.append("]}");
		return buffer.toString();
	}
	
	public GenericRecord nextGenericRecord() throws Exception {
		GenericRecordBuilder rb = new GenericRecordBuilder(schema);
		Map<String, Object> tick = datasource.nextTick();
		tick.forEach((k, v) -> {
			rb.set(k, v);
		});
		GenericRecord record = rb.build();
		return record;
	}

	@Override
	public Map<String, Object> nextTick() throws Exception {
		return datasource.nextTick();
	}

	@Override
	public void open() throws Exception {
		datasource.open();
	}

	@Override
	public void close() throws Exception {
		datasource.close();
	}
	
}
