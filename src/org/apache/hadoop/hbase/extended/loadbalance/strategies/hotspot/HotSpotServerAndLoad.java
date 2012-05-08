/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.extended.loadbalance.strategies.hotspot;

import java.util.Comparator;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.extended.loadbalance.strategies.serverregionload.ServerAndLoad;

public class HotSpotServerAndLoad extends ServerAndLoad implements
		Comparable<HotSpotServerAndLoad> {
	private boolean hotSpot;

	public HotSpotServerAndLoad(final ServerName sn, double load,
			boolean hotSpot) {
		super(sn, load);
		this.hotSpot = hotSpot;
	}

	public boolean isHotSpot() {
		return hotSpot;
	}

	public static Comparator<HotSpotServerAndLoad> ASC_LOAD = new Comparator<HotSpotServerAndLoad>() {
		@Override
		public int compare(HotSpotServerAndLoad l,
				HotSpotServerAndLoad r) {
			int comparison = Double.compare(l.getLoad(), r.getLoad());
			if (comparison == 0)
				comparison = l.getServerName().compareTo(r.getServerName());
			if (comparison > 0)
				return 1;
			else if (comparison < 1)
				return -1;
			else
				return 0;
		}
	};

	public static Comparator<HotSpotServerAndLoad> DESC_LOAD = new Comparator<HotSpotServerAndLoad>() {
		@Override
		public int compare(HotSpotServerAndLoad l,
				HotSpotServerAndLoad r) {
			int comparison = -1 * Double.compare(l.getLoad(), r.getLoad());
			if (comparison == 0)
				comparison = l.getServerName().compareTo(r.getServerName());
			if (comparison > 0)
				return 1;
			else if (comparison < 1)
				return -1;
			else
				return 0;
		}
	};

	@Override
	public int compareTo(HotSpotServerAndLoad other) {
		int comparison = Double.compare(this.getLoad(), other.getLoad());
		return comparison != 0 ? comparison : this.getServerName().compareTo(
				other.getServerName());
	}
}
