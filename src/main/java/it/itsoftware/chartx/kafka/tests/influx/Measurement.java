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
package it.itsoftware.chartx.kafka.tests.influx;

public class Measurement {
	
	private final String name;
	private final String retentionPolicy;
	private final String database;
	
	public Measurement(String name, String retentionPolicy, String database) {
		this.name = name;
		this.retentionPolicy = retentionPolicy;
		this.database = database;
	}
	
	public Measurement(String name, String database) {
		this(name, "default", database);
	}

	public String getName() {
		return name;
	}

	public String getRetentionPolicy() {
		return retentionPolicy;
	}

	public String getDatabase() {
		return database;
	}

	
	@Override
	public String toString() {
		return "Measurement [name=" + name + ", retentionPolicy=" + retentionPolicy + ", database=" + database + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((database == null) ? 0 : database.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((retentionPolicy == null) ? 0 : retentionPolicy.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Measurement other = (Measurement) obj;
		if (database == null) {
			if (other.database != null)
				return false;
		} else if (!database.equals(other.database))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (retentionPolicy == null) {
			if (other.retentionPolicy != null)
				return false;
		} else if (!retentionPolicy.equals(other.retentionPolicy))
			return false;
		return true;
	}

}
