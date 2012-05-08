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
package org.apache.hadoop.hbase.extended.loadbalance.strategies.hotspot.modifiedreadhotspot;

import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.extended.loadbalance.strategies.hotspot.HotSpotRegionLoad;

public class ModifiedReadHotSpotRegionLoad extends HotSpotRegionLoad {

	public ModifiedReadHotSpotRegionLoad(RegionLoad rl) {
		super(rl);

	}

	/**
	 * this is the main method which needs to be changed when we have a new
	 * strategy for calculating the load.
	 */
	public void calculateLoad() {
		double load = this.getReadRequests() / Long.MAX_VALUE;
		// giving twice weight to number of store on the server
		load += 2* this.getNumberOfStores() / Long.MAX_VALUE;
		this.setLoad(load);
	}

}
