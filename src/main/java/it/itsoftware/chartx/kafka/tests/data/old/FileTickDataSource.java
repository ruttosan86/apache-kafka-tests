package it.itsoftware.chartx.kafka.tests.data.old;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Deprecated
public class FileTickDataSource implements TickDataSource {

	private static final String FILENAME = "/home/ruttosan/WORK/Progetti/ITSoftware/chartX/TICK_BY_TICK_UK.csv";
	private static final String SAMPLE_TOPICS_FILE = "/home/ruttosan/WORK/Progetti/ITSoftware/chartX/topics_sample.txt";
	private static DateTimeFormatter formatter;

	static {
		formatter = new DateTimeFormatterBuilder().append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
				.optionalStart().appendPattern(".nnnnnnnnn").optionalEnd().toFormatter();
	}

	private List<String> topics;
	private BufferedReader br;
	private boolean simulateTime;
	private boolean simulateTopics;
	private String fileName;
	private boolean opened;
	
	public FileTickDataSource(String fileName) {
		opened = false;
		this.fileName = fileName;
		simulateTime = false;
		simulateTopics = false;
	}

	public FileTickDataSource() {
		this(FILENAME);
	}

	@Override
	public Map<String, Object> nextTick() throws Exception {
		if(!opened) {
			open();
		}
		String line = br.readLine();
		if (line == null) {
			close();
			open();
			line = br.readLine();
		}
		return parseString(line);
	}

	private Map<String, Object> parseString(String s) {
		Map<String, Object> point = new HashMap<String, Object>();
		String[] fields = s.split(",");
		if (fields.length != 17) {
			return null;
		}
		// TOPIC

		String topic = null;
		if (simulateTopics) {
			Random r = new Random();
			if (topics == null || topics.isEmpty())
				return null;
			topic = topics.get(r.nextInt(topics.size()));
		} else {
			topic = fields[0];
		}
		if (topic.isEmpty())
			return null;
		point.put("topic", topic);
		Long timestamp = null;
		if (simulateTime) {
			timestamp = Instant.now().toEpochMilli() * 1000000;
		} else {
			timestamp = parseToEpochNano(fields[1]);
			if (timestamp == null) {
				return null;
			}
		}
		point.put("time", timestamp);

		Integer trade_num = parseToInteger(fields[2]);
		if (trade_num != null)
			point.put("trade_num", trade_num);

		// BEST_BID
		Float best_bid = parseToFloat(fields[3]);
		if (best_bid != null)
			point.put("best_bid", best_bid);

		// BEST_ASK
		Float best_ask = parseToFloat(fields[4]);
		if (best_ask != null)
			point.put("best_ask", best_ask);

		// TREND_LAT
		Integer trend_lat = parseToInteger(fields[5]);
		if (trend_lat != null)
			point.put("trend_lat", trend_lat);

		// TREND_BRIT
		Integer trend_brit = parseToInteger(fields[6]);
		if (trend_brit != null)
			point.put("trend_brit", trend_brit);

		// LAST_ID
		String last_id = fields[7];
		if (!last_id.isEmpty())
			point.put("last_id", last_id);

		// LAST_TYPE
		String last_type = fields[8];
		if (!last_type.isEmpty())
			point.put("last_type", last_type);

		// AUTOMATIC_TYPE
		String automatic_type = fields[9];
		if (!automatic_type.isEmpty())
			point.put("automatic_type", automatic_type);

		// TRADE_SIGN
		String trade_sign = fields[10];
		if (!trade_sign.isEmpty())
			point.put("trade_sign", trade_sign);

		// TRADE_PRICE
		Float trade_price = parseToFloat(fields[11]);
		if (trade_price != null)
			point.put("trade_price", trade_price);

		// LAST_DATE
		Long last_date = parseToEpochNano(fields[12]);
		if (last_date != null)
			point.put("last_date", last_date);

		// SPECIAL_TRADE
		String special_trade = fields[13];
		if (!special_trade.isEmpty())
			point.put("special_trade", special_trade);

		// TIME_TYPE
		String time_type = fields[14];
		if (!time_type.isEmpty())
			point.put("time_type", time_type);

		// SPIKE_FLAG
		Integer spike_flag = parseToInteger(fields[15]);
		if (spike_flag != null)
			point.put("spike_flag", spike_flag);

		// TRADE_DATE
		Long trade_date = parseToEpochNano(fields[16]);
		if (trade_date != null)
			point.put("trade_date", trade_date);
		return point;
	}

	public void simulateTime() {
		simulateTime = true;
	}

	public void simulateTopics() throws Exception {
		simulateTopics(SAMPLE_TOPICS_FILE);
	}

	public void simulateTopics(int n) {
		ArrayList<String> tmpTopics = new ArrayList<String>();
		for(int i=0; i<n; i++) {
			tmpTopics.add("TIT." + i + ".SIM");
		}
		simulateTopics(tmpTopics);
	}
	
	public void simulateTopics(String fileName) throws Exception {
		simulateTopics(loadTopicsFromFile(fileName));
	}

	public void simulateTopics(ArrayList<String> topics) {
		simulateTopics = true;
		this.topics = topics;
	}

	private ArrayList<String> loadTopicsFromFile(String fileName) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(new File(fileName)));
		ArrayList<String> res = new ArrayList<>();

		String line = null;

		while ((line = br.readLine()) != null) {
			res.add(line);
		}

		br.close();
		return res;
	}

	private Float parseToFloat(String source) {
		if (source.isEmpty()) {
			return null;
		}
		Float value = null;
		try {
			value = new Float(source);
		} catch (NumberFormatException e) {
			return null;
		}
		return value;
	}

	private Integer parseToInteger(String source) {
		if (source.isEmpty()) {
			return null;
		}
		Integer value = null;
		try {
			value = new Integer(source);
		} catch (NumberFormatException e) {
			return null;
		}
		return value;
	}

	private Long parseToEpochNano(String source) {
		if (source.isEmpty()) {
			return null;
		}
		LocalDateTime ldt = null;
		try {
			ldt = LocalDateTime.parse(source, formatter);
		} catch (Exception e) {
			return null;
		}
		long time = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
		time *= 1000000;
		time += System.nanoTime() % 1000000;
		return new Long(time);
	}

	@Override
	public void open() throws Exception {
		br = new BufferedReader(new FileReader(new File(fileName)));
		br.readLine(); 
		opened = true;
	}

	@Override
	public void close() throws Exception {
		if(br != null) {
			br.close();
			opened = false;
		}
	}

}
