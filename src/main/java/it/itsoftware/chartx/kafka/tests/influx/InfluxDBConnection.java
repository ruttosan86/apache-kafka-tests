package it.itsoftware.chartx.kafka.tests.influx;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

public class InfluxDBConnection {

	private static InfluxDBConnection instance = null;
	private static InfluxDB influxDB = null;
	private String user = "root";
	private String pwd = "root";

	private InfluxDBConnection() throws Exception {
		try {
			influxDB = InfluxDBFactory.connect("http://localhost:8086", user, pwd);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new Exception(
					String.format("Failed to Connect to InfluxDB on the localhost:8086 with user='%s'/password='%s'",
							user, "******"));
		}
	}
	
	public static InfluxDBConnection getInstance() throws Exception {
        if(instance == null) {
            instance = new InfluxDBConnection();
        }
        return instance;
    }
	
	public InfluxDB getInfluxDB() {
		return influxDB;
	}
	

}
