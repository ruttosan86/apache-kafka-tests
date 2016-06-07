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

package it.itsoftware.chartx.kafka.tests.data.output;


public class ConsoleOutput<K, T> implements Output<K, T> {

	@Override
	public void write(T record) {
		System.out.println(record.toString());
	}

	@Override
	public boolean open() {
		return true;
	}

	@Override
	public boolean close() {
		return true;
	}

}
