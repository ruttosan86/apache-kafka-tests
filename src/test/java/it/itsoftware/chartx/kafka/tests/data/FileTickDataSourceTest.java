package it.itsoftware.chartx.kafka.tests.data;

import static org.junit.Assert.*;
import it.itsoftware.chartx.kafka.tests.data.FileTickDataSource;

import org.junit.Test;

public class FileTickDataSourceTest {

	@Test
	public void test() {
		System.out.println("New datasource");
		FileTickDataSource datasource  = new FileTickDataSource();
		System.out.println("Setting time simulation");
		datasource.simulateTime();
		try {
			System.out.println("Setting topic simulation");
			datasource.simulateTopics();
		} catch (Exception e) {
			fail();
			return;
		}
		
		System.out.println("Opening datasource");
		try {
			datasource.open();
		} catch (Exception e1) {
			fail();
			return;
		}
		
		System.out.println("Datasource opened");
		for(int i=0; i<100; i++) {
			try {
				System.out.println(datasource.nextTick().toString());
			} catch (Exception e) {
				fail();
				return;
			}
		}
		
		System.out.println("Closing datasource");
		try {
			datasource.close();
		} catch (Exception e) {
			fail();
			return;
		}
		System.out.println("done.");
	}

}
