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

import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import it.itsoftware.chartx.kafka.tests.data.Tick;

public class InfluxDBTickOutput implements TickOutput {

	private InfluxDB db;
	private String dbURL;
	private String dbUser;
	private String dbPassword;
	private String destinationDatabase;
	private String destinationRP;
	private String destinationMeasurement;
	private boolean batchEnabled;
	private int batchSize;
	private boolean closed;

	public InfluxDBTickOutput(String dbURL, String dbUser, String dbPassword, String destinationDatabase,
			String destinationRP, String destinationMeasurement) {
		super();
		this.dbURL = dbURL;
		this.dbUser = dbUser;
		this.dbPassword = dbPassword;
		this.destinationDatabase = destinationDatabase;
		this.destinationRP = destinationRP;
		this.destinationMeasurement = destinationMeasurement;
		this.closed = true;
		this.batchEnabled = false;
		this.batchSize = 1;
	}

	@Override
	public void write(Tick tick) {
		if(closed) {
			open();
		}
		db.write(destinationDatabase, destinationRP, tick.toPoint(destinationMeasurement));
	}

	@Override
	public boolean open() {
		if (!closed) {
			return false;
		}
		db = InfluxDBFactory.connect(dbURL, dbUser, dbPassword);
		if (batchEnabled) {
			db.enableBatch(batchSize, 10, TimeUnit.SECONDS);
		}
		closed = false;
		return true;
	}

	@Override
	public boolean close() {
		if (closed) {
			return false;
		}
		if (batchEnabled) {
			db.disableBatch();
		}
		db = null;
		closed = true;
		return true;
	}

	public boolean enableBatch(int batchSize) {
		if (closed) {
			batchEnabled = true;
			this.batchSize = batchSize;
			return true;
		} else {
			return false;
		}
	}

}
