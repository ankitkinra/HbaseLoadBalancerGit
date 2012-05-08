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
package org.apache.hadoop.hbase.extended.loadbalance.strategies.serverregionload;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.hbase.HServerLoad.RegionLoad;

import com.google.common.primitives.UnsignedBytes;

/***
 * Will contain the raw facts of the load at the time of initialization, and it
 * will also contain the logic of determining the quantum of load Raw parameters
 * will need to be added as the scope expands Also the comparator will compare
 * the load and if the load is same then ties are broken with lexicographic
 * name.
 * 
 * 
 */
public abstract class ModifiedRegionLoad implements
		Comparable<ModifiedRegionLoad> {
	protected static final Comparator<byte[]> BYTE_ARR_COMPARATOR = UnsignedBytes
			.lexicographicalComparator();

	/**
	 * Region Name format is [TableName],[key],[encoded region name] hence it
	 * would be unique across region servers
	 */
	private RegionLoad baseRegionLoad = null;
	private double load = 0.0;

	public ModifiedRegionLoad(RegionLoad rl) {
		this.baseRegionLoad = rl;
		calculateLoad();
	}

	protected abstract void calculateLoad();

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(baseRegionLoad.getName());
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
		ModifiedRegionLoad other = (ModifiedRegionLoad) obj;
		if (!Arrays.equals(this.baseRegionLoad.getName(),
				other.baseRegionLoad.getName()))
			return false;
		return true;
	}

	public static Comparator<ModifiedRegionLoad> ASC_LOAD = new Comparator<ModifiedRegionLoad>() {
		@Override
		public int compare(ModifiedRegionLoad l, ModifiedRegionLoad r) {
			int comparison = Double.compare(l.load, r.load);
			if (comparison == 0)
				comparison = BYTE_ARR_COMPARATOR.compare(
						l.baseRegionLoad.getName(), r.baseRegionLoad.getName());
			if (comparison > 0)
				return 1;
			else if (comparison < 1)
				return -1;
			else
				return 0;
		}
	};

	public static Comparator<ModifiedRegionLoad> DESC_LOAD = new Comparator<ModifiedRegionLoad>() {
		@Override
		public int compare(ModifiedRegionLoad l, ModifiedRegionLoad r) {
			int comparison = -1 * Double.compare(l.load, r.load);
			if (comparison == 0)
				comparison = BYTE_ARR_COMPARATOR.compare(
						l.baseRegionLoad.getName(), r.baseRegionLoad.getName());
			if (comparison > 0)
				return 1;
			else if (comparison < 1)
				return -1;
			else
				return 0;
		}
	};

	@Override
	public int compareTo(ModifiedRegionLoad o) {
		// Load comparison.
		int comparison = Double.compare(this.load, o.load);
		if (comparison == 0)
			comparison = BYTE_ARR_COMPARATOR.compare(
					this.baseRegionLoad.getName(), o.baseRegionLoad.getName());
		return comparison;
	}

	public RegionLoad getBaseRegionLoad() {
		return baseRegionLoad;
	}

	public byte[] getRegionName() {
		return baseRegionLoad.getName();
	}

	public void setRegionName(byte[] regionName) {
		this.baseRegionLoad.setName(regionName);
	}

	public long getReadRequests() {
		return baseRegionLoad.getReadRequestsCount();
	}

	public void setReadRequests(int readRequests) {
		this.baseRegionLoad.setReadRequestsCount(readRequests);
		calculateLoad();
	}

	public long getWriteRequests() {
		return this.baseRegionLoad.getWriteRequestsCount();
	}

	public void setWriteRequests(int writeRequests) {
		this.baseRegionLoad.setWriteRequestsCount(writeRequests);
		calculateLoad();
	}

	public int getNumberOfStores() {
		return this.baseRegionLoad.getStores();
	}

	public void setNumberOfStores(int numberOfStores) {
		this.baseRegionLoad.setStores(numberOfStores);
		calculateLoad();
	}

	public int getNumberOfStorefiles() {
		return this.baseRegionLoad.getStorefiles();
	}

	public void setNumberOfStorefiles(int numberOfStorefiles) {
		this.baseRegionLoad.setStorefiles(numberOfStorefiles);
		calculateLoad();
	}

	public int getStorefileSizeMB() {
		return this.baseRegionLoad.getStorefileIndexSizeMB();
	}

	public void setStorefileSizeMB(int storefileSizeMB) {
		this.baseRegionLoad.setStorefiles(storefileSizeMB);
		calculateLoad();
	}

	public int getMemstoreSizeMB() {
		return this.baseRegionLoad.getMemStoreSizeMB();
	}

	public void setMemstoreSizeMB(int memstoreSizeMB) {
		this.baseRegionLoad.setMemStoreSizeMB(memstoreSizeMB);
		calculateLoad();
	}

	/**
	 * returns the most current load by recalculating the load. It is assumed
	 * that
	 * 
	 * @return
	 */
	public double getLoad() {
		return this.load;
	}

	protected void setLoad(double load) {
		if (load < 0 && load > 1) {
			throw new IllegalArgumentException(
					"Acceptable Load range is [0,1]. you passed = " + load);
		}
		this.load = load;
	}

}
