package it.itsoftware.chartx.kafka.tests.data.serde;

import it.itsoftware.chartx.kafka.tests.data.Tick;

public class TickJSONDeserializer extends JSONDeserializer<Tick>{

	public TickJSONDeserializer() {
		super(Tick.class);
	}
	
}
