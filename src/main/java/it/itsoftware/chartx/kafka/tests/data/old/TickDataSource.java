package it.itsoftware.chartx.kafka.tests.data.old;

import java.util.Map;

@Deprecated
public interface TickDataSource {

	public Map<String, Object> nextTick() throws Exception;
	
	public void open() throws Exception;
	
	public void close() throws Exception;
	
}
