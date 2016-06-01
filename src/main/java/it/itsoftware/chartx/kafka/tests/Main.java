package it.itsoftware.chartx.kafka.tests;

import it.itsoftware.chartx.kafka.tests.data.FileTickDataSource;

public class Main {

	public static void main(String[] args) {
		System.out.println("New datasource");
		FileTickDataSource datasource  = new FileTickDataSource();
		datasource.simulateTime();
		try {
			datasource.simulateTopics();
		} catch (Exception e) {
			return;
		}
		
		System.out.println("Opening datasource");
		try {
			datasource.open();
		} catch (Exception e1) {
			return;
		}
		
		System.out.println("Datasource opened");
		for(int i=0; i<100; i++) {
			try {
				System.out.println(datasource.nextTick().toString());
			} catch (Exception e) {
				return;
			}
		}
		
		System.out.println("Closing datasource");
		try {
			datasource.close();
		} catch (Exception e) {
			return;
		}
		System.out.println("done.");

	}

}
