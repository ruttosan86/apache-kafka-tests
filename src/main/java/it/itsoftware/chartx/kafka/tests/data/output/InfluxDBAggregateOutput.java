package it.itsoftware.chartx.kafka.tests.data.output;

import org.apache.kafka.streams.kstream.Windowed;

import it.itsoftware.chartx.kafka.tests.data.TickAggregation;

public class InfluxDBAggregateOutput extends InfluxDBOutput<Windowed<String>, TickAggregation> {

	public InfluxDBAggregateOutput(String dbURL, String dbUser, String dbPassword, String destinationDatabase,
			String destinationRP, String destinationMeasurement) {
		super(dbURL, dbUser, dbPassword, destinationDatabase, destinationRP, destinationMeasurement);
	}

	@Override
	public void write(TickAggregation record) {
		if (isClosed()) {
			open();
		}
		db.write(destinationDatabase, destinationRP, record.toPoint(destinationMeasurement));
	}

}
