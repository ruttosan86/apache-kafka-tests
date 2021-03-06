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
package it.itsoftware.chartx.kafka.tests.connect.influxdb;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class InfluxDBSinkConnector extends SinkConnector{

	@Override
	public ConfigDef config() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void start(Map<String, String> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Class<? extends Task> taskClass() {
		return InfluxDBSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String version() {
		// TODO Auto-generated method stub
		return null;
	}

}
