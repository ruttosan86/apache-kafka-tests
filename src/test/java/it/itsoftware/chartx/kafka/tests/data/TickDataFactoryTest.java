package it.itsoftware.chartx.kafka.tests.data;

import static org.junit.Assert.*;

import org.junit.Test;

public class TickDataFactoryTest {

	@Test
	public void test() {
		System.out.println("New datasource");
		FileTickDataSource datasource = new FileTickDataSource();
		System.out.println("Setting time simulation");
		datasource.simulateTime();
		System.out.println("Setting topic simulation");
		datasource.simulateTopics(1000);
		TickDataFactory factory = new TickDataFactory(datasource);
		for (int i = 0; i < 100; i++) {
			try {
				System.out.println(factory.nextTick().toString());
			} catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				fail();
			}
		}
	}

}
