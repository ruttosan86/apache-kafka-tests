package it.itsoftware.chartx.kafka.tests;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Stopwatch;

import it.itsoftware.chartx.kafka.tests.data.Tick;
import it.itsoftware.chartx.kafka.tests.data.output.InfluxDBOutput;
import it.itsoftware.chartx.kafka.tests.data.source.SimulatedSmartTickSource;

public class InfluxDBSeriesLoadTest {

	public static void main(String[] args) {

		int nSeries = 500000;
		int nPotentialSeries = 50000000;
		int nTopics = 50000;
		AtomicInteger rate = new AtomicInteger(11000);
		boolean randomize = false;
		if (args.length < 4) {
			System.out.println(
					"Not enough parameters were supplied. Parameters: nSeries nPotentialSeries nTopics rate [random]. Using default values.");
		} else {
			nSeries = Integer.valueOf(args[0]);
			nPotentialSeries = Integer.valueOf(args[1]);
			nTopics = Integer.valueOf(args[2]);
			rate.getAndSet(Integer.valueOf(args[3]));
			if (args.length >= 5) {
				if (args[4].equalsIgnoreCase("random"))
					randomize = true;
			}
		}

		SimulatedSmartTickSource source = new SimulatedSmartTickSource(nSeries, nPotentialSeries, nTopics);
		if (randomize)
			source.randomize();
		
		InfluxDBOutput<String, Tick> output = new InfluxDBOutput<String, Tick>("http://localhost:8086", "root", "root", "serietest", "default",
				"ticks");
		output.enableBatch(10000);
		if(!output.open()) {
			System.err.println("Failed to connect to Influxdb");
			System.exit(-1);
		}
		if(!output.createDatabase()) {
			System.err.println("Failed to create db serietest");
			System.exit(-1);
		}
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		int executionTime = 60 * 60; // time in seconds for data production
		Stopwatch exitTimer = Stopwatch.createStarted();
		Stopwatch timer = Stopwatch.createUnstarted();
		Runnable influxWriter = () -> {
			if (exitTimer.elapsed(TimeUnit.SECONDS) > executionTime) {
				output.close();
				executor.shutdown();
				return;
			}
			System.out.print("Producing " + rate.get() + " ticks...");
			timer.reset();
			timer.start();
			for(int i=0; i<rate.get(); i++) {
				Tick tick = source.next();
				output.write(tick);
			}
			System.out.println( "[Done in " + (timer.elapsed(TimeUnit.MICROSECONDS)/1000) + "ms]");
		};
		
		
		int initialDelay = 0;
		int period = 1000;
		executor.scheduleAtFixedRate(influxWriter, initialDelay, period, TimeUnit.MILLISECONDS);
		
		
		
	}

}
